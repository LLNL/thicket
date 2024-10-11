from glob import glob
import sys
import thicket as th

sys.path.append("/usr/gapps/spot/dev/hatchet-venv/x86_64/lib/python3.9/site-packages/")
sys.path.append("/usr/gapps/spot/dev/hatchet/x86_64/")
sys.path.append("/usr/gapps/spot/dev/thicket-playground-dev/")


usage_str = "Please provide a directory of Caliper files\nUsage: python display_valid_metadata_metric.py <caliper_files_directory>"


def display_valid_metrics_metadata():
    tk = th.Thicket.from_caliperreader(glob(sys.argv[1] + "/**/*.cali", recursive=True))

    print("Valid metadata values:\n")
    for value in tk.metadata.columns:
        print("\t" + value)

    print("\n" + "-" * 30 + "\n")

    print("Valid metric values:\n")
    for value in tk.performance_cols:
        print("\t" + value)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(usage_str)
        exit()
    display_valid_metrics_metadata()
