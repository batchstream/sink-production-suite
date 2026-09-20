import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


spec = importlib.util.spec_from_file_location("server_tests", Path(__file__).with_name("prepare-server-tests.py"))
server_tests = importlib.util.module_from_spec(spec)
spec.loader.exec_module(server_tests)


class ServerTestOverlay(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.server = self.root / "sink"
        self.server.mkdir()
        (self.server / "go.mod").write_text("module github.com/batchstream/sink\n")
        (self.server / "internal/example").mkdir(parents=True)
        self.sources = self.root / "sources"
        (self.sources / "internal/example").mkdir(parents=True)
        self.source = self.sources / "internal/example/roundtrip_test.go"
        self.source.write_text("package example\n")
        self.output = self.root / "evidence/overlay.json"

    def test_resolves_checkout_alias_without_writing_candidate(self):
        alias = self.root / "alias"
        alias.symlink_to(self.server, target_is_directory=True)
        server_tests.prepare(alias, self.sources, self.output)
        mapping = json.loads(self.output.read_text())["Replace"]
        target = self.server / "internal/example/roundtrip_test.go"
        expected = {str(target): str(self.source)}
        self.assertEqual(mapping, expected)
        self.assertFalse(target.exists())

    def test_refuses_to_shadow_candidate_or_replace_production_code(self):
        target = self.server / "internal/example/roundtrip_test.go"
        target.write_text("original")
        with self.assertRaisesRegex(ValueError, "still owns"):
            server_tests.prepare(self.server, self.sources, self.output)
        self.assertEqual(target.read_text(), "original")
        target.unlink()
        self.source.rename(self.source.with_name("implementation.go"))
        with self.assertRaisesRegex(ValueError, "only add tests"):
            server_tests.prepare(self.server, self.sources, self.output)

    def test_refuses_empty_suite_and_missing_candidate_package(self):
        self.source.unlink()
        with self.assertRaisesRegex(ValueError, "no server tests"):
            server_tests.prepare(self.server, self.sources, self.output)
        self.source.write_text("package example\n")
        (self.server / "internal/example").rmdir()
        with self.assertRaisesRegex(ValueError, "package is missing"):
            server_tests.prepare(self.server, self.sources, self.output)
