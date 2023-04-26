from pathlib import Path
import sys
import json

def main(experiment_directory: Path):
    experiment_file_path = experiment_directory.joinpath("experiment.json")
    print(experiment_file_path)
    json_data = json.loads(experiment_file_path.read_text())
    json_data['mlflow']['runs'].reverse()
    with open(experiment_file_path, 'w') as f:
        json.dump(json_data, f, indent=2)
    print("DONE")

if __name__ == '__main__':
    experiment_directory = sys.argv[1]
    main(Path.cwd().joinpath(experiment_directory))
