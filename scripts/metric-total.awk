# Sum all stores for one counter, using Store-labelled series.
# A counter vector has no samples before its first observation.
index($1, metric_name "{") == 1 {
    total += $2
}
END {
    printf "%.0f\n", total
}
