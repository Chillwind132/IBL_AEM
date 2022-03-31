from csv import reader
from logging import exception
from os import read
import time
import requests
from bs4 import BeautifulSoup
import csv  
import time



def create_csv_header():
    refernce_header = ['URL', 'Status', 'Inbound links']
    file = open('output.csv', 'w', newline='')
    write = csv.writer(file)
    write.writerows([refernce_header])


def calculations(snapshot, row_list, data_dict):
    # We need to check if Target_url is on this page, if it is add it to the respective dict
    #Keep snapshots of the page and then iterate through them?

    for item in row_list: 
        t = row_list[item]["Response"]
        #data_dict.setdefault(item, []).append(t)
        for key in snapshot:
            if item in snapshot[key]["List"]:
                data_dict.setdefault(item, []).append(key)
                #data_dict.setdefault(key, []).append(response)
                #time.sleep(0.5)
                print("FOUND")
                
    

def populate_data(data_dict, snapshot, full_target_url, row_list):
    with open(r'output.csv', 'a', newline='') as f:
        
        writer = csv.writer(f)
        for key, value in data_dict.items():
            response = row_list[key]["Response"]
            writer.writerow([key, response, value])
                
def read_data(rooturl):
    with open('data.csv', 'r') as read_obj:
        exit = False
        response_ok = "None"
        
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        if header != None:
            for row in csv_reader:

                if str(row[0]) == "":
                    print("EMPTRY ROW")
                    calculations(snapshot, row_list, data_dict)
                    populate_data(data_dict, snapshot,
                                  full_target_url, row_list)
                    return

                full_url_list = []
                author_url_string = str(row[0])
                url_1 = author_url_string.replace("/content/pwc", rooturl)
                full_target_url = url_1 + ".html"

                try:
                    response = requests.get(full_target_url, allow_redirects=False, timeout=5)
                    soup = BeautifulSoup(response.text, 'html.parser')
                    hyperlink ='a'
                    for link in soup.find_all(hyperlink):
                        href_link = link.get('href')
                        try:
                            if href_link[0] == "/":
                                try:
                                    if href_link[1] == "/":
                                        link_updated = href_link[1:]

                                        href_link = rooturl + link_updated
                                        direction = "Internal"
                                    else:
                                        href_link = rooturl + href_link
                                except Exception:
                                        href_link = rooturl + href_link
                            else:
                                direction = "External"
                            full_url_list.append(href_link)
                        except Exception:
                            print("Exception")
                    
                    if response.status_code == 301:
                        response_ok = "301 redirect"
                        print("REDIRECT", full_target_url)
                    elif response.ok:
                        response_ok = 'OK'
                        print("VALID", full_target_url)
                        snapshot[full_target_url] = {}
                        snapshot[full_target_url]["List"] = full_url_list
                        snapshot[full_target_url]["Response"] = response_ok

                    else:
                        response_ok = "Broken"
                        print("INVALID", full_target_url)
                        #data_dict.setdefault(full_target_url, []).append('Broken URL')

                except Exception: #ReadTimeout
                    print("Timeout Exception")

            
                row_list[full_target_url] = {}
                row_list[full_target_url]["Response"] = response_ok
                

if __name__ == "__main__":
    start = time.time()
    create_csv_header()

    data_dict = {}
    snapshot = {}
    row_list = {}
     
    rooturl = 'https://www.pwc.com'  # https://www.pwc.com https://www-pwc-com-dpe-staging.pwc.com
    
    read_data(rooturl)

    end = time.time()
    print(end - start)
    
    print("Done")
    

# https://www.pwc.com/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form
#/content/pwc/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form