import sys, xml.etree.ElementTree as ET
root = ET.fromstring(sys.stdin.read())
print(f'{"TIME":<6} {"LINE":<8} {"TRAIN":<12} {"PLATFORM":<10} {"TYPE":<5} DESTINATION')
print('-' * 100)
for s in root.findall('s'):
    tl = s.find('tl')
    for event_type, event in [('DP', s.find('dp')), ('AR', s.find('ar'))]:
        if event is not None:
            pt = event.attrib.get('pt', '')
            time = f'{pt[6:8]}:{pt[8:10]}' if len(pt) >= 10 else ''
            line = event.attrib.get('l', '')
            train = f"{tl.attrib.get('c','')} {tl.attrib.get('n','')}" if tl is not None else ''
            platform = event.attrib.get('pp', '')
            path = event.attrib.get('ppth', '').split('|')[-1] if event.attrib.get('ppth') else ''
            print(f'{time:<6} {line:<8} {train:<12} {platform:<10} {event_type:<5} {path}')