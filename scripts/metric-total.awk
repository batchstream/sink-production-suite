# Sum all stores for one counter, including legacy series without labels.
# A counter vector has no samples before its first observation.
$1 == metric_name || index($1, metric_name "{") == 1 {
    total += $2
}
END {
    printf "%.0f\n", total
}
