#!/bin/bash

OUT_DIR="/root/Python/stats"
mkdir -p "$OUT_DIR"

NOW=$(date +"%Y-%m-%d_%H-%M-%S")
PREFIX="$OUT_DIR/server_load_$NOW"

# === CPU ===
sar -u -s 03:00 > "$PREFIX-cpu.txt"
gnuplot -persist <<EOF
set terminal png size 800,400
set output "$PREFIX-cpu.png"
set title "CPU Usage (%)"
set xlabel "Time"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M"
set ylabel "Usage (%)"
set grid
plot "$PREFIX-cpu.txt" using 1:3 title "%user" with lines, \
     "" using 1:5 title "%iowait" with lines, \
     "" using 1:8 title "%idle" with lines
EOF

# === RAM ===
sar -r -s 03:00 > "$PREFIX-ram.txt"
gnuplot -persist <<EOF
set terminal png size 800,400
set output "$PREFIX-ram.png"
set title "Memory Usage (used vs free)"
set xlabel "Time"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M"
set ylabel "kB"
set grid
plot "$PREFIX-ram.txt" using 1:4 title "used" with lines, \
     "" using 1:2 title "free" with lines
EOF

# === Disk I/O ===
sar -d -s 03:00 > "$PREFIX-disk.txt"
gnuplot -persist <<EOF
set terminal png size 800,400
set output "$PREFIX-disk.png"
set title "Disk Activity (tps)"
set xlabel "Time"
set xdata time
set timefmt "%H:%M:%S"
set format x "%H:%M"
set ylabel "Transfers/sec"
set grid
plot "$PREFIX-disk.txt" using 1:3 title "read KB/s" with lines, \
     "" using 1:4 title "write KB/s" with lines
EOF

echo "✅ Графіки збережено в $OUT_DIR (з префіксом $PREFIX)"
