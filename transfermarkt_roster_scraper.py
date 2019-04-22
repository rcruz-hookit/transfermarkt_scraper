import sys
import time
import urllib.request       #Python 3.7.2
from bs4 import BeautifulSoup
import json
from multiprocessing import Pool

#SETUP THREADING/MULTI-PROCESSING
#http://blog.adnansiddiqi.me/how-to-speed-up-your-python-web-scraper-by-using-multiprocessing/



# FUNCTION TO RETRIEVE THE URL PAGE VIA A GET REQUEST
# python 3.7.2 - ABSOLUTE MINIMUM TO WORK:
#html_request = urllib.request.urlopen(pUrl)
#html_doc = html_request.read()
#return html_doc
def getHtmlPage(pUrl):
    try:
        headers = {}
        headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17'
        req = urllib.request.Request(pUrl, headers=headers)
        with urllib.request.urlopen(req) as response:
            the_page = response.read()

        return the_page

    except urllib.error.URLError as e:
        print(e.reason)

    except:
        print("Error retreiving html_page")


#<div class="spielerdaten ">
#<table class="auflistung">
#    <tr>
#        <th>Name in Home Country /<br>Full Name:</th>
#        <td>Roberto Júnior Fernández Torres</td>
#    </tr>
def getPlayerFullName(pUrl):

    playerFullName = ''

    try:
        fullUrl = "https://www.transfermarkt.co.uk" + pUrl

        # RETRIEVE HMTL PAGE
        html_doc = getHtmlPage(fullUrl)

        # PARSE THE HTML WITH BEAUTIFUL SOUP
        #html_soup = BeautifulSoup(html_doc, 'html.parser')
        html_soup = BeautifulSoup(html_doc, 'lxml')
        #strainedSoup = SoupStrainer("div")
        #print(strainedSoup)

        # RETRIEVE THE DATA FROM BEAUTIFUL SOUP
        teamSoup = html_soup.find('div','spielerdaten')
        playerFullName = teamSoup.table.tr.td.text
        print(playerFullName)
    except:
        playerFullName = 'ErrorRetrievingPlayerFullName'

    return playerFullName


# LOGIC:
# 1) POPULATE ARRAY OF URLs to PULL
# 2) FOR EACH URL, PARSE PAGE WITH BEAUTIFUL SOUP
# 3) FOR EACH PLAYER FOUND, PRINT OUT THE PLAYER NAME
def transfermarkt_roster_scraper(pUrl):

    playerList = []
    playerUrls = []

    try:
        # RETRIEVE HMTL PAGE
        html_doc = getHtmlPage(myURL)

        # PARSE THE HTML WITH BEAUTIFUL SOUP
        #html_soup = BeautifulSoup(html_doc, 'html.parser')
        html_soup = BeautifulSoup(html_doc, 'lxml')

        # GET TEAM NAME
        teamSoup = html_soup.find('div','dataName')
        teamName = teamSoup.h1.span.text

        # GET DIV WITH THIS EXACT POSITION:
        myPlayers = html_soup.findAll('div', 'di nowrap')   #<div class="di nowrap">

        # FOR EACH DIV, EXTRACT THE PLAYER NAME; PRINT OUT THE STRING, REMOVING WHITESPACE
        counter = 2

        for player in myPlayers:
            #print(counter%2)
            if counter%2 == 0:
                #time.sleep(0.25)                                #quarter second delay timer
                #playerUrl = player.a["href"]                    #/roberto-fernandez/profil/spieler/107318
                playerUrls.append(player.a["href"])                    #/roberto-fernandez/profil/spieler/107318
                #playerNickname = player.a.text
                #playerFullName = getPlayerFullName(playerUrl)
                #playerList.append(teamName + ',' + playerNickname + ',' + playerFullName)  #playerNickName is in ANCHOR tag
            counter += 1


        #PARALLEL PROCESS:
        with Pool(10) as p:
            playerList = p.map(getPlayerFullName, playerUrls)
        


    except:
        print('Error scraping URL:' + str(pUrl))

    return playerList


def extractQueryStringParameter(pEvent, pParameterName):

    returnValue = ''

    if 'queryStringParameters' in pEvent:
        queryStringParamsDict = pEvent['queryStringParameters']
        if queryStringParamsDict is not None:
            for item in queryStringParamsDict:
                if item == pParameterName:
                    returnValue = pEvent['queryStringParameters'][item]

    return returnValue


# AMAZON AWS LAMBDA HANDLER
def lambda_handler(event, context):   
    
    myUrl = extractQueryStringParameter(event, 'url')    
    rosterData = transfermarkt_roster_scraper(myUrl)

    return {
        'statusCode': 200,
        'body': json.dumps(rosterData)
    }



if __name__== "__main__":

    # POPULATE ARRAY; LIST OF URLs WE NEED
    urlList = []
    #urlList.append("https://www.transfermarkt.com/kashima-antlers/startseite/verein/2241")
    #urlList.append("https://www.transfermarkt.com/kashiwa-reysol/startseite/verein/6632")
    #urlList.append("https://www.transfermarkt.com/kawasaki-frontale/startseite/verein/9598")
    #urlList.append("https://www.transfermarkt.com/sagan-tosu/startseite/verein/22177")
    #urlList.append("https://www.transfermarkt.com/sanfrecce-hiroshima/startseite/verein/2697")
    #urlList.append("https://www.transfermarkt.com/shimizu-s-pulse/startseite/verein/1062")
    urlList.append("https://www.transfermarkt.com/urawa-red-diamonds/startseite/verein/828")
    
    for myURL in urlList:   
        rosterData = transfermarkt_roster_scraper(myURL)
        print(json.dumps(rosterData))
