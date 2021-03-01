import pyodbc
import requests
import pandas as pd
import regex as re
import numpy as np

server = 'DESKTOP-4DQ59T4\SQLEXPRESS' 
database = 'PublicWhipTest'
username = 'Creator' 
password = 'radio84'
cnxn = pyodbc.connect(r'Driver={SQL Server};Server=DESKTOP-4DQ59T4\SQLEXPRESS;Database=PublicWhipTest;Trusted_Connection=yes;')
cursor = cnxn.cursor()

url = "https://www.publicwhip.org.uk/divisions.php"
page = requests.get(url)

divisions = page.text

divisions = re.search(r"\"Sort by turnout\">Turnout<\/a><table class=\"votes\">(.*?)About the Project</h3>",
                      divisions,
                      re.DOTALL)

divisions = re.sub(r"<td class=", "|", str(divisions))

divisions = divisions.split(r"|")

divisions = pd.DataFrame(divisions, columns=["Text"])

pd.set_option('display.max_colwidth', 400)

divisions["Wanted"] = \
    divisions["Text"].str.contains('.*"commons".*', regex=True)

divisions.query('Wanted == True', inplace=True)

divisions["Text"].str.strip()

for index, row in divisions.iterrows():
    divisions.loc[index, "date"] = \
        re.search(r'\d{4}-\d{2}-\d{2}',
                  divisions.loc[index, "Text"]).group(0)
    divisions.loc[index, "number"] = \
        re.search(r'&number=(.*?)"',
                  divisions.loc[index, "Text"]).group(1)

divisions.drop(columns=["Text", "Wanted"], inplace=True)

divisions.sort_index(0, ascending=False, inplace=True)

divisions["url"] = 'https://www.publicwhip.org.uk/division.php?date='\
                   + divisions["date"] + '&house=commons&number=' + \
                   divisions["number"]

for index, row in divisions.iterrows():
    page = requests.get(divisions.loc[index, "url"])
    divisions.loc[index, "data"] = page.text

for index, row in divisions.iterrows():
    m = re.search(r"<tr class=\"headings\"><td>Name</td><td>Constituency</td><td>Party</td>.*?</td></tr>(.*?)</table>",
                  divisions.loc[index, "data"],
                  re.DOTALL, re.MULTILINE)
    if m == None:
        divisions.loc[index, "rebel_rows"] = "ND"
    else:
        divisions.loc[index, "rebel_rows"] = m.group(1)

for index, row in divisions.iterrows():
    m = re.search(r"<h1>\s(.*?)\s&#",
                  divisions.loc[index, "data"],
                  re.DOTALL, re.MULTILINE)
    if m == None:
        divisions.loc[index, "Topic"] = "ND"
    else:
        divisions.loc[index, "Topic"] = m.group(1)

rebel_data = divisions


rebel_data["rebel_rows"].replace(r'\n', '', regex=True, inplace=True)

rebel_data["splitme"] = \
    rebel_data["rebel_rows"].str.findall(r'class=(.*?)</tr')

for index, row in rebel_data.iterrows():
    if not rebel_data.loc[index, "splitme"]:
        rebel_data.loc[index, "splitme"] = ["No Rebellion"]

rebel_data["splitme"].tolist()
rebel_data = rebel_data.explode("splitme").reset_index()
rebel_data.drop(columns=["data", "rebel_rows", "index"],
                inplace=True)

for index, row in rebel_data.iterrows():
    m = re.search(r"house=commons\">(.*?)</a>.*?house=commons\">(.*?)</a></td><td>(.*?)</td><td>(.*?)</td>", rebel_data.loc[index, "splitme"])
    if m == None:
        rebel_data.loc[index, "MP"] = np.nan
        rebel_data.loc[index, "Constituency"] = np.nan
        rebel_data.loc[index, "Party"] = np.nan
        rebel_data.loc[index, "Vote"] = np.nan
    else:
        rebel_data.loc[index, "MP"] = m.group(1).strip()
        rebel_data.loc[index, "Constituency"] = m.group(2).strip()
        rebel_data.loc[index, "Party"] = m.group(3).strip()
        rebel_data.loc[index, "Vote"] = m.group(4).title().strip()

for index, row in rebel_data.iterrows():
    m2 = re.search(r"(Lab|Con|LDem|DUP|Green|SNP|SDLP|Independent|PC|Alliance)\s", str(rebel_data.loc[index, "Party"]))
    if m2 != None:
        rebel_data.loc[index, "Party"] = m2.group(1)
    else:
        rebel_data.loc[index, "Party"] = np.nan

rebel_data.drop(columns=["splitme"],
                inplace=True)

rebel_data.rename(columns={"date": "Vote_Date", "number": "Number", "url": "URL"}, inplace=True)
rebel_data = rebel_data[["Vote_Date", "Number", "MP", "Constituency", "Party", "Vote", "Topic", "URL"]]
rebel_data = rebel_data.astype({"Number": "int32"})
rebel_data = rebel_data.fillna(value="-")

for index, row in rebel_data.iterrows():
    cursor.execute("INSERT INTO Rebels (Vote_Date, Number, MP, Constituency, Party, Vote, Topic, URL) values(?,?,?,?,?,?,?,?)", row.Vote_Date, row.Number, row.MP, row.Constituency, row.Party, row.Vote, row.Topic, row.URL)
cnxn.commit()
cursor.close()