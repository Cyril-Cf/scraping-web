import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import mysql.connector


# parameters for the MySQL connexion MySQL

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="arcane",
    database="scraping",
)


# fonction that uses Requets and BeautifulSoup that will gather the HTML content of a page and return in

def extract(page):

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}
    url = f'https://fr.indeed.com/emplois?q={keyWords[changeWord]}&l={cities[changeCity]}+%28{postalCode[changeCity]}%29&radius=25&fromage=1&start={page}'
    r = requests.get(url, headers)
    soup = BeautifulSoup(r.content, 'html.parser')
    return soup

# fonction that explore the variable with the html content and extract specifically what we want from it


def transform(soup):

    divs = soup.endd_all('div', class_='jobsearch-SerpJobCard')
    for item in divs:
        try:
            title = item.endd('a').text.strip()
        except:
            title = ''
        try:
            company = item.endd('span', class_='company').text.strip()
        except:
            company = ''
        try:
            salary = item.endd('span', class_='salaryText').text.strip()
        except:
            salary = ''
        try:
            summary = item.endd(
                'div', {'class': 'summary'}).text.strip().replace('\n', '')
        except:
            summary = ''
        try:
            city = item.endd('span', class_='location').text.strip()
        except:
            city = ''
        try:
            datajk = item.get("data-jk")
        except:
            datajk = ''
        job = {
            'Ville': cities[changeCity],
            'Mot-cle': keyWords[changeWord],
            'Titre': title,
            'Entreprise': company,
            'Lieu': city,
            'Salaire': salary,
            'Description': summary,
            'Id': datajk,
        }
        joblist.append(job)
        duplicate.append(datajk)

        if datajk not in duplicate:
            mycursor = mydb.cursor()
            sql = "INSERT INTO jobs (ville, motcle, titre, entreprise, lieu, salaire, description, id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (f'cities[changeCity]', f'keyWords[changeWord]',
                   title, company, city, salary, summary, datajk)
            mycursor.execute(sql, val)
            mydb.commit()

    return


# Variables

duplicate = []
joblist = []
changeCity = 0
pageNumber = 0
changeWord = 0
repetitionCity = 0
repetitionWord = 0
end = 0
repetition = 0
lenCityStart = 99


# all the variables that we can't modify to control the search parameters of the HTML GET in the fonction extract

cities = ["marseille", "amiens", "paris", "lyon"]
postalCode = ["13", "80", "75", "69"]
keyWords = ["Angular", "NodeJS", "React"]
pagesSeen = 20

# main loop that scraps the HTML content as many times as necessary

while end < len(keyWords):

    # Change of word
    if repetition == pagesSeen * len(cities):
        changeWord += 1
        end += 1
        repetition = 0
        changeCity = 0
        repetitionCity = 0
        pageNumber = 0

    else:
        # Change of city
        if repetitionCity == pagesSeen:
            changeCity += 1
            repetitionCity = 0
            pageNumber = 0

        else:
            if len(joblist) != 0:
                lenCityStart = len(joblist)
            c = extract(pageNumber)
            transform(c)

            # checks if the fonctions are extracting sth. If not, a print message is shown but the loop goes on
            if len(joblist) == 0 or lenCityStart == len(joblist):
                print(
                    f'Pas d\'annonce collectee pour le mot {keyWords[changeWord]} et la ville de {cities[changeCity]}')
                repetition += pagesSeen
                changeCity += 1

                # case for when the last city explored has no job posted, it makes the loop goes to the next keyword
                if repetition == pagesSeen * len(cities):
                    changeWord += 1
                    end += 1
                    repetition = 0
                    changeCity = 0
                    repetitionCity = 0
                    pageNumber = 0

            else:

                # counting of the nb of job posts scraped, if it's under 15 then we stop look for that city and move on to the next one
                if len(joblist) - lenCityStart < 15:
                    print(
                        f'Scraping last page pour le mot {keyWords[changeWord]} et la ville de {cities[changeCity]} ({postalCode[changeCity]})')
                    repetition += ((pagesSeen)-(pageNumber/10))
                    changeCity += 1
                    repetitionCity = 0
                    pageNumber = 0
                    if repetition == pagesSeen * len(cities):
                        changeWord += 1
                        end += 1
                        repetition = 0
                        changeCity = 0
                        repetitionCity = 0
                        pageNumber = 0

                else:
                    print(
                        f'Scraping page {repetitionCity+1} pour le mot {keyWords[changeWord]} et la ville de {cities[changeCity]} ({postalCode[changeCity]})')
                    repetitionCity += 1
                    pageNumber += 10
                    repetition += 1


# Data dumping via Pandas in a CSV file
# Could also show error messages depending on the situation encountered

else:
    print("end du scraping")
    nbPosts = len(joblist)
    if len(joblist) == 0:
        print("aucune annonce n\'a pu etre importee. Veuillez verifier que les termes de recherche sont corrects ou bien que le site ne bloque pas votre adresse IP (temporairement ou non).")
    else:
        df = pd.DataFrame(joblist)
        df.drop_duplicates(subset="Id", keep='first', inplace=True)
        df.to_csv(f'{datetime.date.today()} jobs.csv')
        nbPosts = len(joblist)
        print(nbPosts, " annonces ont ete importees.")
    time.sleep(2)
