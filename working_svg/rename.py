try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import re

svg_file = 'crewe_td.svg'
tree = ET.ElementTree(file=svg_file)
root = tree.getroot()
track = 700

for element in root.iterfind('.//{http://www.w3.org/2000/svg}g[@id="base_layer"]'):
    for sub_elem in element.iterfind('.//{http://www.w3.org/2000/svg}g[@id="crewe_basic_track_layout"]'):
        for e in sub_elem.iterfind('.//{http://www.w3.org/2000/svg}path'):
            id = e.get('id')
            if 'track_' in id:
                numbers = re.findall('[0-9]', id)
                numbers = ''.join(numbers)
                label = 'Routing Track {}'.format(numbers)
                e.set('label', label)
                e.set('{http://www.inkscape.org/namespaces/inkscape}label', label)
                pass
            else:
                id = track
                e.set('id', 'track_{}'.format(id))
                label = 'Routing Track {}'.format(id)
                e.set('label', label)
                e.set('{http://www.inkscape.org/namespaces/inkscape}label', label)
                track += 1

tree.write(svg_file)