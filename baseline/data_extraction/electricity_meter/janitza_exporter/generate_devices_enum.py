import csv

from janitza_exporter import JanitzaExporter


def export_devices():
    exporter = JanitzaExporter()
    devices = exporter.devices

    devices.to_csv(
        'devices.csv',
    )

def generate_enum_from_csv(csv_file: str, output_file: str) -> None:
    export_devices()

    name_map = {}

    with open(csv_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name_map[parse_name(row['janitza_name'])] = row['janitza_id']

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('from enum import Enum\n\n\n')
        f.write('class DEVICES(Enum):\n')
        # f.write('    ALL = 0\n')
        f.write('\n'.join(f'    {name} = {id_}' for name, id_ in name_map.items()) + '\n')


def parse_name(s: str) -> str:
    umlaut_map = {
        'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss',
        'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue'
    }

    s = s.replace(' ', '_')
    s = s.replace('.', '_')
    s = s.replace('-', '_')

    s = ''.join(umlaut_map.get(c, c) for c in s)
    return s

if __name__ == '__main__':
    # Run once to generate the file
    export_devices()
    generate_enum_from_csv('devices.csv', 'devices.py')
