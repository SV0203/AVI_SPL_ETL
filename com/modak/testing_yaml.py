import yaml

from yaml.loader import SafeLoader

# Open the file and load the file
with open('dwh_mapping.yaml') as f:
    data = yaml.load(f, Loader=SafeLoader)
    def recursion_data(refdata):
        for key, dwh_mapping_value in refdata.items():
            #print(key, dwh_mapping_value)
            if isinstance(dwh_mapping_value, dict):
                for tables, otherdetails in dwh_mapping_value.items():
                    if tables == 'entity':
                        print(key, tables)
                    for tables,othertables in otherdetails.items():
                        if tables == 'entity':
                            print( othertables)
                # else:
                #     print(dwh_mapping_value)
                #

    recursion_data(data)


