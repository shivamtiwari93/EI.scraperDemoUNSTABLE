from bs4 import BeautifulSoup
import requests
import requests.exceptions
import urllib
from urllib.parse import urlsplit
from collections import deque
import re

import csv

import random

import time

##############

with open("output.csv", "w") as csvfile2:
    writer = csv.writer(csvfile2)
    writer.writerows([['emails']])

    results = []
    with open("data.csv","r") as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            results.append(row)

    #print(results)

    StartNum = 0

    for i in range(len(results)-StartNum):

        if(i < 2 or i > 279):

            searchString = results[i+StartNum][0]
            searchString = searchString + ' ' + results[i+StartNum][2]
            searchStringUrlEncoded = urllib.parse.quote(searchString)
            searchUrl = 'https://www.google.com/search?q=' + searchStringUrlEncoded + '&btnI'

            # a queue of urls to be crawled
            new_urls = deque([searchUrl])
            #new_urls = [searchUrl]

            # a set of urls that we have already crawled
            processed_urls = set()

            # a set of crawled emails
            emails = set()

            count = 0

            # process urls one by one until we exhaust the queue
            while len(new_urls):

                # move next url from the queue to the set of processed urls
                url = new_urls.popleft()
                processed_urls.add(url)


                # extract base url to resolve relative links
                parts = urlsplit(url)
                base_url = "{0.scheme}://{0.netloc}".format(parts)
                path = url[:url.rfind('/')+1] if '/' in parts.path else url

                # get url's content
                print("Processing %s" % url)
                try:
                    response = requests.get(url)
                except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError):
                    # ignore pages with errors
                    continue

                # extract all email addresses and add them into the resulting set
                new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))
                emails.update(new_emails)

                # create a beutiful soup for the html document
                soup = BeautifulSoup(response.text)

                # find and process all the anchors in the document
                for anchor in soup.find_all("a"):
                    # extract link url from the anchor
                    link = anchor.attrs["href"] if "href" in anchor.attrs else ''
                    # resolve relative links
                    if link.startswith('/'):
                        link = base_url + link
                    elif not link.startswith('http'):
                        link = path + link
                    # add the new url to the queue if it was not enqueued nor processed yet
                    if not link in new_urls and not link in processed_urls:
                        new_urls.append(link)

                print(emails)
                count = count + 1
                if count == 20:
                    break
                time.sleep(random.randint(0,2))
            
            emailString = ''

            for i2 in emails:
                emailString += i2
                emailString += ' , '

            if emailString == '':
                emailString = '------------------'
                
            writer.writerows([[emailString]])
            num = str(i+StartNum)
            print('### DONE ###' + num + '### DONE ###')

        ##############
        
    csvfile.close()
    csvfile2.close()
