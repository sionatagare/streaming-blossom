import os, sys, git

git_root_dir = git.Repo(".", search_parent_directories=True).working_tree_dir
sys.path.insert(0, os.path.join(git_root_dir, "benchmark"))
sys.path.insert(0, os.path.join(git_root_dir, "src", "fpga", "utils"))
from main_benchmark_decoding import *
from hardware.frequency_optimization.circuit_level_final.run import (
    Configuration as CircuitLevelFinalConfig,
)
from hardware.decoding_speed.circuit_level_common import *

this_dir = os.path.dirname(os.path.abspath(__file__))

# SAMPLES = 10_000  # draft
SAMPLES = 1_00_000  # final — capped below the ~58985 threshold where streaming
                  # solver enters a stale-state it can't recover from via soft reset
                  # (hardware registers like liveConflictReg/accConflict/archivedRegs
                  # survive the RESET instruction; fixing would require RTL work).
                  # Statistics converge well before 50k samples.

# Streaming (`USE_STREAMING=1`): RTL elastic archive BRAM depth. This is **not** SAMPLES (you still run
# 100k rounds with one finite BRAM); it must be large enough that the archive pipeline never runs out
# of commit slots for your graph + streaming pattern (see DistributedDual). Too small → hang in
# `find_obstacle`. Larger → more FPGA RAM. Override: `export ARCHIVE_DEPTH=256`.
STREAMING_DEFAULT_ARCHIVE_DEPTH = 128

if os.environ.get("USE_STREAMING") and "ARCHIVE_DEPTH" not in os.environ:
    os.environ["ARCHIVE_DEPTH"] = str(STREAMING_DEFAULT_ARCHIVE_DEPTH)


def plot_data_until_d9(this_dir: str):
    name = os.path.basename(this_dir)
    plt.cla()
    for d in d_vec:
        if d > 9:
            continue
        with open(os.path.join(this_dir, f"d_{d}.txt"), "r", encoding="utf8") as f:
            p_data = []
            average_latency_data = []
            for line in f.readlines():
                line = line.strip("\r\n ")
                if line == "" or line.startswith("#"):
                    continue
                spt = line.split(" ")
                assert len(spt) == 3
                p_data.append(float(spt[0]))
                average_latency_data.append(float(spt[1]))
        plt.loglog(p_data, average_latency_data, "o-", label=f"$d = {d}$")
    plt.ylim(1e-7, 3e-4)
    plt.ylabel("decoding latency")
    plt.xlabel("physical error rate")
    plt.legend()
    plt.savefig(os.path.join(this_dir, f"{name}.pdf"))


if __name__ == "__main__":
    data = []
    for d in d_vec:
        latency_vec = []
        for p in p_vec:
            benchmarker = DecodingSpeedBenchmarker(
                this_dir=this_dir,
                configuration=CircuitLevelFinalConfig(d=d),
                p=p,
                samples=SAMPLES,
                use_layer_fusion=True,
                enable_detailed_print=False,
            )
            result = benchmarker.run()
            latency_vec.append(result.latency)
        data.append(latency_vec)
        save_data(data, this_dir)
    plot_data_until_d9(this_dir)
