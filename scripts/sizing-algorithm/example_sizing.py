#!/usr/bin/env python3
"""Print an example VSS sizing report using default input parameters.

Requires instance types with pricing to be passed in.  For a quick demo
you can define a small list inline (see ``_DEMO_INSTANCES`` below).

Usage::

    python3 example_sizing.py
"""

from vss_sizing import InstanceType, SizingInput, compute_sizing

# Minimal demo instances — replace with API-fetched data in production.
_DEMO_INSTANCES: list[InstanceType] = [
    InstanceType("r7g.4xlarge",   16,  128,   3.427,   2.116),
    InstanceType("r7g.8xlarge",   32,  256,   6.854,   4.232),
    InstanceType("r7g.12xlarge",  48,  384,  10.282,   6.348),
    InstanceType("r7g.16xlarge",  64,  512,  13.709,   8.464),
    InstanceType("r7i.24xlarge",  96,  768,  25.402,  15.6832),
    InstanceType("r7i.48xlarge", 192, 1536,  50.803,  31.3656),
]


def main() -> None:
    inp = SizingInput()
    result = compute_sizing(inp, _DEMO_INSTANCES)

    print("=" * 60)
    print("  ScyllaDB VSS Sizing — Default Parameters")
    print("=" * 60)
    print()
    print(f"  Vectors        : {inp.num_vectors:>14,}")
    print(f"  Dimensions     : {inp.dimensions:>14,}")
    print(f"  Target QPS     : {inp.target_qps:>14,}")
    print(f"  Recall         : {inp.recall:>13}%")
    print(f"  K              : {inp.k:>14,}")
    print(f"  Quantization   : {inp.quantization.name:>14}")
    print(f"  Metadata/vec   : {inp.metadata_bytes_per_vector:>12} B")
    print(f"  Filter columns : {inp.filtering_columns:>14}")
    print()
    print(result.summary)
    print()


if __name__ == "__main__":
    main()
