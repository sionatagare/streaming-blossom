import argparse
import os
import signal
import sys
import time
import subprocess
from subprocess import TimeoutExpired


def _post_firmware_idle_sec(session_timeout: float) -> float:
    """
    After we see firmware output, we still use an idle timeout so a hung UART
    does not block forever. Use the same budget as the outer ``timeout`` passed
    by the caller (e.g. decoding benchmarks use 3600s): streaming / heavy decode
    can go many minutes without a serial line (no per-sample prints), and a hard
    900s cap was stopping capture before ``[exit]`` / final benchmarker lines.

    Env: TTY_POST_FIRMWARE_IDLE_SEC — override idle seconds after firmware (float, min 30).
    """
    if "TTY_POST_FIRMWARE_IDLE_SEC" in os.environ:
        return max(30.0, float(os.environ["TTY_POST_FIRMWARE_IDLE_SEC"]))
    # Match session cap (decoding uses 3600s); a 900s cap was cutting long streaming runs.
    return float(max(120.0, session_timeout))


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


def _terminate_command_tree(child: subprocess.Popen) -> None:
    """
    ``make run_a72`` launches ``xsdb`` which holds JTAG. SIGTERM on ``make`` alone
    often orphans ``xsdb``, leaving the cable/session wedged so the *next* run
    hangs before ``------- start of build parameters -------``.
    """
    if os.name == "nt":
        child.terminate()
        return
    try:
        os.killpg(os.getpgid(child.pid), signal.SIGTERM)
    except (ProcessLookupError, PermissionError):
        child.terminate()


def _kill_command_tree(child: subprocess.Popen) -> None:
    if os.name == "nt":
        child.kill()
        return
    try:
        os.killpg(os.getpgid(child.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        child.kill()


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
            # Never use PIPE here without a concurrent drain thread: ``xsdb`` can print
            # megabytes to stdout/stderr; a full pipe blocks xsdb before the ELF runs, so
            # the next ``read()`` on the serial tty file never sees the firmware banner
            # (looks like an unexplained hang).
            popen_kw: dict = {
                "stdin": subprocess.DEVNULL,
                "stdout": subprocess.DEVNULL if silent else sys.stdout,
                "stderr": subprocess.DEVNULL if silent else None,
            }
            # New session so we can killpg(make) and take down xsdb + children (POSIX).
            if os.name != "nt" and not shell:
                popen_kw["start_new_session"] = True
            child = subprocess.Popen(command, shell=shell, cwd=cwd, **popen_kw)
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
        force_killed = False
        try:
            child.wait(timeout=join_sec)
        except TimeoutExpired:
            force_killed = True
            print(
                f"[warning] get_ttyoutput: subprocess still running after {join_sec}s; "
                "terminating process group (make + xsdb). "
                "Set TTY_SUBPROCESS_JOIN_SEC to adjust; "
                "TTY_COOLDOWN_AFTER_KILL_SEC pause before next use (default 5)."
            )
            _terminate_command_tree(child)
            try:
                child.wait(timeout=30)
            except TimeoutExpired:
                _kill_command_tree(child)
                try:
                    child.wait(timeout=10)
                except TimeoutExpired:
                    pass
        if force_killed:
            cooldown = float(os.environ.get("TTY_COOLDOWN_AFTER_KILL_SEC", "5"))
            if cooldown > 0:
                # Let hw_server / cable release before the next ``make run_a72``.
                time.sleep(cooldown)
        # make/xsdb stdout is discarded when silent (see PIPE deadlock note above).
        if not silent and child.stdout is not None:
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
