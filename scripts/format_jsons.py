import glob
import json


def decode_stacked(document, pos=0, decoder=json.JSONDecoder()):
    while True:
        match = decoder.raw_decode(document, pos)
        yield match[0]
        pos = match[1]

# Find all JSON files in the specified folder
for filename in glob.glob('/home/sdiaz/Documents/udacity-3/stedihumanbalanceanalyticsdata/accelerometer/*.json'):
    with open(filename, 'r') as infile, open(filename + '_new.json', 'w') as outfile:
        data = infile.read()

        try:
            for obj in decode_stacked(data):
                outfile.write(json.dumps(obj) + '\n')  # write each json object in new line
        except json.JSONDecodeError:
            print(f"File {filename} has issues with JSON structure.")
