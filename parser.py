import argparse
import glob
from dotenv import load_dotenv
import os
import xml.etree.ElementTree as ET
import shutil
import psycopg2


# Load environment variables from the .env file (if present)
load_dotenv()

def monotabular(path, concept_name):
    tree = ET.parse(path)
    root = tree.getroot()
    section = root

    table_name = str(section.tag).lower()
    print("Populating table " + concept_name + "_" + table_name)
    for entry in section:
        columns = []
        values = []
        for field in entry:
            columns.append(field.tag)
            values.append(field.text)
        clmn = ', '.join(['"{}"'.format(value) for value in columns])
        sql = 'INSERT INTO "{}" ({}) VALUES ({})'.format(concept_name + "_" + table_name, clmn,
                                                         ','.join(['%s'] * len(columns)))
        cursor.execute(sql, values)


def polytabular(path, concept_name):
    tree = ET.parse(path)
    root = tree.getroot()
    for section in root:
        table_name = str(section.tag).lower()
        print("Populating table " + table_name)
        for entry in section:
            columns = []
            values = []
            for field in entry:
                columns.append(field.tag)
                values.append(field.text)
            clmn = ', '.join(['"{}"'.format(value) for value in columns])
            sql = 'INSERT INTO "{}" ({}) VALUES ({})'.format(concept_name + "_" + table_name, clmn,
                                                             ','.join(['%s'] * len(columns)))
            cursor.execute(sql, values)


# parser = argparse.ArgumentParser()
# parser.add_argument('-d', '--directory', type=str, required=True,
#                     help='directory containing the unzipped nhsbsa_dmd... folders')
# args = parser.parse_args()


# Access environment variables as if they came from the actual environment
username = os.getenv('DATABASE_USER')
password = os.getenv('DATABASE_PASSWORD')
host = os.getenv('DATABASE_HOST')
port = os.getenv('DATABASE_PORT')
db_name = os.getenv('DATABASE_NAME')
rootpath = os.getenv('TRUD_FILE_EXTRACT_LOCATION')

# Example usage
print(f'DATABASE_USER: {username}')
print(f'DATABASE_PASSWORD: {password}')
print(f'DATABASE_HOST: {host}')
print(f'DATABASE_port: {port}')
print(f'DATABASE_db_name: {db_name}')

cnx = psycopg2.connect(user=username, password=password, host=host, port=port, dbname=db_name)
cnx.autocommit = True
cursor = cnx.cursor()

table_structure_path = os.path.dirname(os.path.realpath(__file__)) + '/dmd_structure.sql'

concepts = [
    ("f_lookup2_3{}.xml", "lookup", polytabular),
    ("f_ingredient2_3{}.xml", "ingredient", monotabular),
    ("f_vtm2_3{}.xml", "vtm", monotabular),
    ("f_vmp2_3{}.xml", "vmp", polytabular),
    ("f_amp2_3{}.xml", "amp", polytabular),
    ("f_vmpp2_3{}.xml", "vmpp", polytabular),
    ("f_ampp2_3{}.xml", "ampp", polytabular)
]
os.chdir(rootpath)
dmddirs = glob.glob("nhsbsa_dmd_*_*")
if len(dmddirs) == 0:
    raise Exception("No dm+d directories in the path given")
for dmddir in dmddirs:
    print(f"Processing {dmddir}")
    if not os.path.isdir(dmddir):  # we don't want to match zip
        print("Skipping " + dmddir)
        continue

    schema_name = dmddir[7:-6].replace('.', '_')  # no '.' in schema name so we don't have to use quotes
    print("Creating schema " + schema_name)
    cursor.execute('CREATE SCHEMA IF NOT EXISTS "{}";'.format(schema_name))
    cursor.execute('SET search_path TO "{}";'.format(schema_name))

    print("Importing table structure")
    with open(table_structure_path, 'r') as f:
        cursor.execute(f.read())

    print("Parsing xml...")
    os.chdir(dmddir)
    for con in concepts:
        filename = glob.glob(con[0].format("*"))[0]
        print("Parsing concept " + con[1])
        con[2](filename, con[1])

    # SQL command to add the column
    #cursor.execute('ALTER TABLE lookup_supplier ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE vmp_vmps ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE amp_amps ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE vtm_virtual_therapeutic_moieties ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE lookup_unit_of_measure ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE lookup_ont_form_route ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE vmp_ont_drug_form ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE lookup_route ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE vmp_drug_route ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE lookup_form ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')
    #cursor.execute('ALTER TABLE vmp_drug_form ADD COLUMN "isSynchronized" BOOLEAN DEFAULT FALSE;')



    #data insert to process nsh queue list
    insert_command = "INSERT INTO medicine.nhs_medicine_api_data_process (schema_name, status) VALUES (%s, %s);"
    data = (format(schema_name), 1)  # Example data

    # Execute the command
    cursor.execute(insert_command, data)
    print("Data inserted successfully into 'medicine.nhs_medicine_api_data_process' table.")

    cnx.commit()
    shutil.rmtree(rootpath+"/"+dmddir)
    print("Done")

cursor.close()
cnx.close()
