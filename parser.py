import argparse
import glob
import os
import xml.etree.ElementTree as ET

import psycopg2


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


parser = argparse.ArgumentParser()
parser.add_argument('-d', '--directory', type=str, required=True,
                    help='directory containing the unzipped nhsbsa_dmd... folders')
parser.add_argument('-u', '--username', type=str, required=True,
                    help='username of your PostgreSQL database')
parser.add_argument('-p', '--password', type=str, required=True,
                    help='password of the PostgreSQL database corresponding to the username')
parser.add_argument('-H', '--host', type=str, default='localhost',
                    help='host of the PostgreSQL database')
parser.add_argument('-P', '--port', type=int, default=5432,
                    help='port of the PostgreSQL database')
args = parser.parse_args()

rootpath = args.directory
username = args.username
password = args.password
host = args.host
port = args.port

cnx = psycopg2.connect(user=username, password=password, host=host, port=port, dbname='postgres')
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

    cnx.commit()
    print("Done")

cursor.close()
cnx.close()
