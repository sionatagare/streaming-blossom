import argparse
import os
import sys
import time
import subprocess
from subprocess import TimeoutExpired


def _post_firmware_idle_sec(session_timeout: float) -> float:
    """
    After we see firmware output, we still use an idle timeout so a hung UART
    does not block forever. The old fixed 30s was too short for decoding sweeps
    (quiet stretches, or serial backpressure). Scale with the caller's session
    timeout and allow an env override on servers without redeploying code.

    Env: TTY_POST_FIRMWARE_IDLE_SEC — idle seconds after firmware starts (float, min 30).
    """
    if "TTY_POST_FIRMWARE_IDLE_SEC" in os.environ:
        return max(30.0, float(os.environ["TTY_POST_FIRMWARE_IDLE_SEC"]))
    # e.g. session_timeout=3600 -> 900s cap, 120s floor
    return float(min(900.0, max(120.0, session_timeout / 3.0)))


def _firmware_markers_present(tty_so_far: str) -> bool:
    """Match embedded banner even if a line is split across UART reads or slightly garbled."""
    t = tty_so_far
    return (
        "start of build parameters" in t
        or "end of build parameters" in t
        or "hardware_info:" in t
        or "USE_LAYER_FUSION:" in t
    )


script_dir = os.path.dirname(os.path.abspath(__file__))
default_ttyfile = os.path.join(script_dir, "ttymicroblossom")


def get_ttyoutput(
    filename=default_ttyfile,
    command="",
    exit_word="[exit]",
    timeout=180,
    silent=True,
    cwd=None,
    shell=False,
):
    tty_output = ""
    command_output = ""
    if isinstance(exit_word, str):
        exit_words = [exit_word]
    else:
        exit_words = list(exit_word)
    with open(filename, "r") as file:
        file.seek(0, 2)  # seek to the end
        if command != "":
            stdout = subprocess.PIPE if silent else sys.stdout
            child = subprocess.Popen(command, shell=shell, cwd=cwd, stdout=stdout)
        last = time.time()
        seen_firmware_output = False
        while True:
            time.sleep(0.1)
            content = file.read()
            if not silent:
                print(content, end="")
            tty_output += content.strip("\x00")
            if content != "":
                last = time.time()
            if not seen_firmware_output and _firmware_markers_present(tty_output):
                seen_firmware_output = True
            effective_timeout = (
                _post_firmware_idle_sec(float(timeout))
                if seen_firmware_output
                else float(timeout)
            )
            if time.time() - last > effective_timeout:
                print(f"[warning] ttyoutput timeouted after {effective_timeout}s")
                break
            if any(w in tty_output for w in exit_words):
                break
    if command != "":
        # TTY capture can stop (idle timeout or exit word) while make/xsdb is still running;
        # an unconditional wait() would hang the benchmark host forever.
        join_sec = float(os.environ.get("TTY_SUBPROCESS_JOIN_SEC", "120"))
        try:
            child.wait(timeout=join_sec)
        except TimeoutExpired:
            print(
                f"[warning] get_ttyoutput: subprocess still running after {join_sec}s; "
                "sending SIGTERM (set TTY_SUBPROCESS_JOIN_SEC to adjust)"
            )
            child.terminate()
            try:
                child.wait(timeout=30)
            except TimeoutExpired:
                child.kill()
        if silent:
            command_output = child.stdout.read().decode("utf-8")
    return tty_output, command_output


def main(args=None):
    parser = argparse.ArgumentParser(
        description="Get TTY Output, monitoring the output given a trigger command, exit word and timeout"
    )
    parser.add_argument(
        "-f",
        "--file",
        default=default_ttyfile,
        help="the path of the tty output file, see README.md for more information",
    )
    parser.add_argument(
        "-c",
        "--command",
        default="",
        help="execute this command while waiting for the file output",
    )
    parser.add_argument(
        "-e",
        "--exit-word",
        default="[exit]",
        help="when seeing this word, the program is returned",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        default="180",
        help="timeout value if exit word is not found, default to 3min. any active data will reactive a new timeout",
    )
    parser.add_argument("-s", "--silent", action="store_true", help="no printing")
    args = parser.parse_args(args=args)

    # print(args)
    filename = args.file
    exit_word = args.exit_word
    timeout = float(args.timeout)
    silent = args.silent
    command = args.command

    return get_ttyoutput(filename, command, exit_word, timeout, silent)


if __name__ == "__main__":
    main()
