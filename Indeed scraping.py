import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import time
import mysql.connector


# Parametre de connexion MySQL

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="arcane",
  database="scraping",
)


# Fonction avec Requests + BS qui va chercher le contenu HTML complet d'une page et le renvoi dans une variable

def extract(page):
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}
    url = f'https://fr.indeed.com/emplois?q={motcles[changementMot]}&l={villes[changementVille]}+%28{codepostal[changementVille]}%29&radius=25&fromage=1&start={page}'
    r = requests.get(url, headers)
    soup = BeautifulSoup(r.content, 'html.parser')
    return soup


# Fonction avec Requests + BS qui parcourt la variable contenant le code HTML pour extraire du texte en fonction des parametres de recherche

def transform(soup):
    
    divs = soup.find_all('div', class_ = 'jobsearch-SerpJobCard')
    for item in divs:
        try:
            title = item.find('a').text.strip()
        except:
            title = ''
        try:
            company = item.find('span', class_ ='company').text.strip()
        except:
            company = ''
        try:
            salary = item.find('span', class_ = 'salaryText').text.strip()
        except:
            salary = ''
        try:
            summary = item.find('div', {'class' : 'summary'}).text.strip().replace('\n', '')
        except:
            summary = ''
        try:
            lieu = item.find('span', class_ = 'location').text.strip()
        except:
            lieu = ''               
        try:
            datajk = item.get("data-jk")           
        except:
            datajk = ''             
        job = {
            'Ville': villes[changementVille],
            'Mot-cle': motcles[changementMot],
            'Titre': title,
            'Entreprise': company,
            'Lieu': lieu,
            'Salaire': salary,
            'Description': summary,
            'Id': datajk,
        }
        joblist.append(job)
        duplicate.append(datajk)
        
        if datajk not in duplicate:
            mycursor = mydb.cursor()
            sql = "INSERT INTO jobs (ville, motcle, titre, entreprise, lieu, salaire, description, id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (f'villes[changementVille]',f'motcles[changementMot]', title, company, lieu, salary, summary, datajk)
            mycursor.execute(sql, val)
            mydb.commit()
            
    return


# Variables necessaires a la boucle

duplicate = []
joblist = []
changementVille = 0
pageNumber = 0
changementMot = 0
repetitionVille = 0
repetitionMot = 0
fin = 0
repetition = 0
lenvillestart = 99


# Variables a modifier pour ajouter des parametres ou changer le nb de pages parcourues pour chaque requete
# Si j'ajoute une ville je dois ajouter le code postal dans le tableau correspondant
# Nb total de pages scrapees = len(villes) * len(motcles) * pagesParcourues. Ce nombre doit etre inferieur a 500 (limite des sites pour la meme IP)

villes = ["marseille","amiens","paris","lyon"]
codepostal = ["13","80","75","69"]
motcles = ["Angular","NodeJS","React"]
pagesParcourues = 20


# Iteration du script, scrapping du nb de pages predefini pour les dernieres 24h pour chaque mot-cles dans chaque ville

while fin < len(motcles):

# Changement de mot-cle
    if repetition == pagesParcourues * len(villes):
        changementMot += 1
        fin += 1
        repetition = 0
        changementVille = 0
        repetitionVille = 0
        pageNumber = 0

    else:
        # Changement de ville
        if repetitionVille == pagesParcourues:
            changementVille += 1
            repetitionVille = 0
            pageNumber = 0
            
        else:          
            if len(joblist) != 0:
                lenvillestart = len(joblist)
            c = extract(pageNumber)
            transform(c)

            # Verification que les fonctions ont pu extraire quelque chose. Si la variable est vide, une erreur est renvoyee mais le script poursuit tout de meme
            if len(joblist) == 0 or lenvillestart == len(joblist):
                print(f'Pas d\'annonce collectee pour le mot {motcles[changementMot]} et la ville de {villes[changementVille]}') 
                repetition += pagesParcourues
                changementVille += 1


                # Cas ou c'est la derniere ville qui est sans annonce et pour amorcer le passage au mot suivant
                if repetition == pagesParcourues * len(villes):
                    changementMot += 1
                    fin += 1
                    repetition = 0
                    changementVille = 0
                    repetitionVille = 0
                    pageNumber = 0                 
               
            else:
                
                # comptage du nb d'annonce scrapees sur la page si c'est inferieur a 15 on passe a la ville ou au mot suivant sans explorer + de page
                
                if len(joblist)- lenvillestart < 15:
                    print(f'Scraping last page pour le mot {motcles[changementMot]} et la ville de {villes[changementVille]} ({codepostal[changementVille]})')
                    repetition += ((pagesParcourues)-(pageNumber/10))
                    changementVille += 1
                    repetitionVille = 0
                    pageNumber = 0
                    if repetition == pagesParcourues * len(villes):
                        changementMot += 1
                        fin += 1
                        repetition = 0
                        changementVille = 0
                        repetitionVille = 0
                        pageNumber = 0                     
                     
                else:
                    print(f'Scraping page {repetitionVille+1} pour le mot {motcles[changementMot]} et la ville de {villes[changementVille]} ({codepostal[changementVille]})')
                    repetitionVille += 1
                    pageNumber += 10     
                    repetition += 1
            
# Dumping des donnees via Pandas en format CSV
# Message d'erreur correspondant aux situations rencontrees

else: 
    print("fin du scraping")
    nbannonces = len(joblist)
    if len(joblist) == 0:
        print("aucune annonce n\'a pu etre importee. Veuillez verifier que les termes de recherche sont corrects ou bien que le site ne bloque pas votre adresse IP (temporairement ou non).")     
    else:    
        df = pd.DataFrame(joblist)
        df.drop_duplicates(subset ="Id", keep = 'first', inplace=True)
        df.to_csv(f'{datetime.date.today()} jobs.csv')
        nbannonces = len(joblist)
        print(nbannonces, " annonces ont ete importees.")
    time.sleep(2)


