import json
import random
import string

def random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))

def generate_random_spec(num_columns=10):
    spec = {
        "ColumnNames": [],
        "Offsets": [],
        "FixedWidthEncoding": random.choice(["utf-8", "windows-1252", "ascii"]),
        "IncludeHeader": random.choice([True, False]),
        "DelimitedEncoding": random.choice(["utf-8", "windows-1252", "ascii"])
    }

    for i in range(num_columns):
        spec["ColumnNames"].append(f"f{i+1}")
        spec["Offsets"].append(random.randint(1, 20))

    return spec

def save_spec_to_file(spec, filename="input/random_spec.json"):
    with open(filename, 'w') as f:
        json.dump(spec, f, indent=4)

if __name__ == "__main__":
    # Generate a random spec
    random_spec = generate_random_spec()

    # Print the spec
    print(json.dumps(random_spec, indent=4))

    # Save the spec to a file
    save_spec_to_file(random_spec)
    print(f"Random spec saved to random_spec.json")