"""Print a readable summary of the Spark standalone master's JSON state.

Reads the master UI's /json/ endpoint on stdin and prints its workers and
applications. Used to tell "the worker is gone" apart from "the worker is
there but the job asks for more than it has".
"""

import json
import sys


def main():
    """Print the workers and applications from the master state on stdin."""
    state = json.load(sys.stdin)

    print("--- workers ---")
    for worker in state.get("workers", []):
        print(
            "  {id}  cores={cores} used={coresused}  mem={mem:.2f}G used={memused:.2f}G".format(
                id=worker.get("id"),
                cores=worker.get("cores"),
                coresused=worker.get("coresused"),
                mem=worker.get("memory", 0) / 2**30,
                memused=worker.get("memoryused", 0) / 2**30,
            )
        )

    print("--- active applications ---")
    for app in state.get("activeapps", []):
        print(
            "  {id}  {name}  cores={cores}  mem={mem:.2f}G".format(
                id=app.get("id"),
                name=app.get("name"),
                cores=app.get("cores"),
                mem=app.get("memory", 0) / 2**30,
            )
        )

    print("--- last completed applications ---")
    for app in state.get("completedapps", [])[-5:]:
        print("  {id}  {name}".format(id=app.get("id"), name=app.get("name")))


if __name__ == "__main__":
    main()