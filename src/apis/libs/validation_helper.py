import re


def parse_csv(lead_ids):
    for ll in list(dict.fromkeys(lead_ids.split(','))):
        # ignore blanks
        if ll:
            yield int(ll)


def not_valid_csv(strg, search=re.compile(r'[^0-9,]').search):
    return bool(search(strg))
