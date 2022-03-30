import sqlite3
import json
import os
import pandas as pd
import re




conn = sqlite3.connect('happiness.db')
c = conn.cursor()

#Create Countries table
c.execute("""CREATE TABLE countries (id INTEGER PRIMARY KEY AUTOINCREMENT,country varchar, images_file text, image_url text, alpha2 text, alpha3 text,
country_code integer, iso_3166_2 text, region text, sub_region text, intermediate_region text, region_code integer,
sub_region_code integer, intermediate_region_code integer
)""")


#Read countries json file
myJsonFile = open('Data_Files\Data Files\countries_continents_codes_flags_url.json','r')
json_data = myJsonFile.read()
countries_json_obj = json.loads(json_data)


#Insert Data in Countries table
for country in countries_json_obj:
    c.execute("insert into countries (country,images_file,image_url,alpha2,alpha3,country_code,iso_3166_2,region,sub_region,intermediate_region,region_code,sub_region_code,intermediate_region_code) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
              [country['country'], country['images_file'], country['image_url'], country['alpha-2'], country['alpha-3'],
               country['country-code'], country['iso_3166-2'], country['region'], country['sub-region'], country['intermediate-region'],
               country['region-code'], country['sub-region-code'], country['intermediate-region-code']])
    conn.commit()



#Read CSV files


csv_file_path = os.getcwd()+'\\Data_Files\\Data Files\\csv_files\\'
csv_files = []
for file in os.listdir(csv_file_path):

    if(file.endswith('.csv')):
        csv_files.append(file)


#Create DataFrame of csv files
df = {}
df_list = []
for file in csv_files:

    df[file] = pd.read_csv(csv_file_path + file)

    file_name_str = str(file)
    report_year = re.findall('(\d{4})', file_name_str)

    df[file].loc[:, 'year'] = str(report_year[0])

    df[file].columns = [x.lower().replace(" ","_").replace("?","") \
                            .replace("-","_").replace("(","").replace(")","").replace("..","_").replace(".","_") \
                        for x in df[file].columns]

    for x in df[file].columns:
        col_name = str(x)
        if col_name.endswith("_"):
            c_col_name = col_name
            col_name = col_name[:-1]
            df[file].rename(columns = ({c_col_name: col_name}),inplace=True)

    df[file].rename(columns=({"economy_gdp_per_capita": "gdp_per_capita"}), inplace=True)
    df[file].rename(columns=({"score": "happiness_score"}), inplace=True)
    df[file].rename(columns=({"freedom": "freedom_to_make_life_choices"}), inplace=True)
    df[file].rename(columns=({"country_or_region": "country"}), inplace=True)
    df[file].rename(columns=({"healthy_life_expectancy": "health_life_expectancy"}), inplace=True)
    df_list.append(df[file])

result = pd.concat(df_list)


replacements = {
    'object': 'varchar',
    'float64': 'float',
    'int64': 'int',
    'datetime64': 'timestamp',
    'timedelta64[ns]': 'varchar'
}

col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(result.columns, result.dtypes.replace(replacements)))


conn = sqlite3.connect('happiness.db')
c = conn.cursor()

#Create countries_happiness record table
c.execute("""CREATE TABLE countries_happiness (ID INTEGER PRIMARY KEY AUTOINCREMENT, %s);""" % (col_str))
conn.commit()

#Insert data from csv files to countries_happiness table
result.to_sql(name="countries_happiness", con=conn, if_exists='append', index=False)




#Question 3 - SQL Query to CSV

SQL_Query_Q3 = pd.read_sql_query('''select ch.year,c.country,c.image_url,c.region_code,c.region,ch.gdp_per_capita,ch.family,ch.social_support,ch.health_life_expectancy,ch.freedom_to_make_life_choices,ch.generosity,ch.perceptions_of_corruption from countries c inner join countries_happiness ch on c.country=ch.country''', conn)

df2 = pd.DataFrame(SQL_Query_Q3)
df2.to_csv (r'Exported_csv\exported_data_q3.csv', index = False)

conn.close()