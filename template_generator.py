"""
WorldQuant BRAIN Template Generator

Reads operator templates from operators.txt (one per line, each containing
{DATA} as a placeholder) and generates two levels of combinations:

  Depth-1: every operator as-is             e.g. ts_rank({DATA}, 10)
  Depth-2: every (outer, inner) pair        e.g. rank(ts_rank({DATA}, 10))

Output is written to templates.txt, one expression per line.

Usage:
    python template_generator.py
    python template_generator.py --operators-file my_ops.txt --output my_templates.txt
"""

import argparse
import sys

PLACEHOLDER = "{DATA}"


def load_operators(path: str) -> list[str]:
    ops = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if PLACEHOLDER not in line:
                print(
                    f"WARNING: skipping '{line}' — does not contain {PLACEHOLDER}",
                    file=sys.stderr,
                )
                continue
            ops.append(line)
    return ops


def generate(operators: list[str]) -> list[str]:
    templates: list[str] = []
    seen: set[str] = set()

    def add(expr: str) -> None:
        if expr not in seen:
            seen.add(expr)
            templates.append(expr)

    # Depth-1: operators themselves
    for op in operators:
        add(op)

    # Depth-2: substitute {DATA} in outer with the full inner expression
    for outer in operators:
        for inner in operators:
            combined = outer.replace(PLACEHOLDER, inner)
            # Skip trivial self-substitution when outer == inner and
            # it produces the same string as depth-1
            if combined != outer:
                add(combined)

    return templates


def main() -> None:
    p = argparse.ArgumentParser(
        description="Generate BRAIN alpha template combinations from an operator list.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--operators-file", default="operators.txt",
                   help="Input file with one operator template per line")
    p.add_argument("--output", default="templates.txt",
                   help="Output file — one template expression per line")
    args = p.parse_args()

    operators = load_operators(args.operators_file)
    if not operators:
        print(f"ERROR: no valid operators found in '{args.operators_file}'.", file=sys.stderr)
        sys.exit(1)

    templates = generate(operators)
    depth1 = len(operators)
    depth2 = len(templates) - depth1

    with open(args.output, "w") as f:
        for t in templates:
            f.write(t + "\n")

    print(
        f"Generated {len(templates)} templates "
        f"({depth1} depth-1, {depth2} depth-2) → {args.output}"
    )


if __name__ == "__main__":
    main()
