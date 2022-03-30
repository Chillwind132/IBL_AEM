from csv import reader
from os import read
import time
import requests
from bs4 import BeautifulSoup
import csv  

def create_csv_header():
    refernce_header = ['URL', 'Status', 'Inbound links']
    file = open('output.csv', 'w', newline='')
    write = csv.writer(file)
    write.writerows([refernce_header])

def calculations(snapshot, full_target_url, data_dict):
    # We need to check if Target_url is on this page, if it is add it to the respective dict
    #Keep snapshots of the page and then iterate through them?
    
    for key in snapshot:
        data_1 = snapshot[key]
        #print("Stored data:", data_1, "Full URL:", full_target_url)
        for item in data_1["List"]:

            if item == full_target_url:
                
                #data_dict[key] = {}
                data_dict.setdefault(key, []).append(full_target_url)
                #data_dict.setdefault(key, []).append(response)
                time.sleep(1)
                print("FOUND")
                time.sleep(1)
    

def populate_data(row_list, snapshot, full_target_url):
    with open(r'output.csv', 'a', newline='') as f:
        
        writer = csv.writer(f)
        for key, value in row_list.items():
            response = snapshot[key]["Response"]
            writer.writerow([key, response, value])
                
def read_data(rooturl):
    with open('data.csv', 'r') as read_obj:
        exit = False
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        if header != None:
            for row in csv_reader:
                if exit:
                    populate_data(data_dict, snapshot, full_target_url)
                    return
                full_url_list = []
                author_url_string = str(row[0])
                url_1 = author_url_string.replace("/content/pwc", rooturl)
                full_target_url = url_1 + ".html"
                print (full_target_url)
                time.sleep(1)
                try:
                    response = requests.get(full_target_url, allow_redirects=False)

                    if response.status_code == 301:
                        response_ok = "301 redirect"
                        print("REDIRECT", full_target_url)
                    elif response.ok:
                        response_ok = 'OK'
                        print("VALID", full_target_url)
                        redirect_url = ""
                    else:
                        response_ok = "Broken"
                        print("INVALID", full_target_url)
        
                except Exception:
                    exit = True
               
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
                        print("Test:", href_link, full_target_url)

                    except Exception:
                        print("Exception")

                snapshot[full_target_url] = {}
                snapshot[full_target_url]["List"] = full_url_list
                snapshot[full_target_url]["Response"] = response_ok
                calculations(snapshot, full_target_url, data_dict)
            
                
if __name__ == "__main__":

    create_csv_header()

    data_dict = {}
    snapshot = {}
    
    rooturl = 'https://www-pwc-com-dpe-staging.pwc.com'
    
    read_data(rooturl)
    
    print("Done")

# https://www.pwc.com/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form
#/content/pwc/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form