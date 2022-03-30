from csv import reader
import time
import requests
from bs4 import BeautifulSoup
import csv  


def calculations(full_url_list, data_dict, full_target_url):  
    # We need to check if Target_url is on this page, if it is add it to the respective dict
    #Keep snapshots of the page and then itirate through them?
    inbound_links = ""
    if full_target_url in full_url_list:
        print("LINK FOUND", full_target_url)
        
        inbound_links = inbound_links + full_target_url
        data_dict[full_target_url] = {}
        data_dict[full_target_url]["inbound"] = inbound_links 
        print(data_dict)
        time.sleep(2)

def populate_data(row_list):
    with open(r'output.csv', 'a', newline='') as f:
        

        writer = csv.writer(f)
        writer.writerow(row_list)
                

if __name__ == "__main__":


    data_dict = {}
    timeout_list = []
    
    rooturl = 'https://www.pwc.com'
    full_url_list = []
    snapshot = {}

    i = 0
    with open('data.csv', 'r') as read_obj:
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        
        if header != None:
            
            for row in csv_reader:
            
                author_url_string = str(row[0])
                url_1 = author_url_string.replace("/content/pwc/ca/en/","https://www.pwc.com/ca/en/")
                full_target_url = url_1 + ".html"
                print (full_target_url)
                time.sleep(1)
               
                response = requests.get(
                    full_target_url, timeout=10)
                print("VALID", full_target_url)
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

                calculations(full_url_list, data_dict, full_target_url)
                snapshot[full_target_url] = {}
                snapshot[full_target_url]["List"] = full_url_list
                i = i + 1

    print("Done")

                

                




# https://www.pwc.com/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form
#/content/pwc/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form