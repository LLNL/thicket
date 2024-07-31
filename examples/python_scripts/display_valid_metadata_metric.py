from glob import glob
import sys
import thicket as th

sys.path.append("/usr/gapps/spot/dev/hatchet-venv/x86_64/lib/python3.9/site-packages/")
sys.path.append("/usr/gapps/spot/dev/hatchet/x86_64/")
sys.path.append("/usr/gapps/spot/dev/thicket-playground-dev/")


def display_valid_metrics_metadata():
    tk = th.Thicket.from_caliperreader(glob(sys.argv[1] + "/**/*.cali", recursive=True))

    print("Valid metadata values:\n")
    for value in tk.metadata.columns:
        print("\t" + value)

    print("\n" + "-" * 30 + "\n")

    print("Valid metric values:\n")
    for value in tk.dataframe.columns:
        print("\t" + value)


if __name__ == "__main__":
    display_valid_metrics_metadata()
