import requests
from urllib.request import urlopen
from zipfile import ZipFile
import json
import xmltodict
import pandas as pd
import logging as log
url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"
def write_to_xml_file(url):
  '''
  this method take input string url
  return : write content of response to xml file
  '''
  try:
    response = requests.get(url)
  except as Exception e:
    raise("url is not found")
  with open('feed.xml', 'wb') as file:
      file.write(response.content
def parsed_xml_get_zip(filename):
  """
  filename : string 'filepath of the xml file'
  write zip file to working directory and read zip file and extract zip file
  return : string 'zip file url'
  """
  with open(filename) as xml_file:
      data_dict = xmltodict.parse(xml_file.read())
  json_data = json.dumps(data_dict)
  json_data = json.loads(json_data)
  log.info("xml data into json {}".format(json_data))
  try:
    list_data = json_data["response"]["result"]["doc"]
    log.info("doc data into list {}".format(list_data))
    zip_file = ""
    for data in list_data:
      for inner_data in data['str']:
        if ".zip" in inner_data["#text"]:
          zip_file =  inner_data["#text"]
          break
    zipresp = urlopen(zip_file)
    log.info("writing zip file current working directory")
    tempzip = open("/content/tempfile.zip", "wb")
    tempzip.write(zipresp.read())
    tempzip.close()
    zf = ZipFile("/content/tempfile.zip")
    zf.extractall(path = '/content/')
    zf.close()
  except Exception as e:
    log.info("key not found {}".format(e))
    raise("key not found error")
def write_to_csv(filename):
  """
  filename : filepath of the extracted zip file
  parsed xml file and write back to csv file
  """
  main_data = []
  with open(filename) as xml_file:
      data_dict = xmltodict.parse(xml_file.read())
  json_data = json.dumps(data_dict)
  json_data = json.loads(json_data)
  try:
    for data in json_data["BizData"]["Pyld"]['Document']['FinInstrmRptgRefDataDltaRpt']['FinInstrm']:
      new_data = {}
      new_data['id'] = data.get("TermntdRcrd",{}).get("FinInstrmGnlAttrbts",{}).get("Id","")
      new_data['full_name'] = data.get("TermntdRcrd",{}).get("FinInstrmGnlAttrbts",{}).get("FullNm","")
      new_data['ClssfctnTp'] = data.get("TermntdRcrd",{}).get("FinInstrmGnlAttrbts",{}).get("ClssfctnTp","")
      new_data['CmmdtyDerivInd'] = data.get("TermntdRcrd",{}).get("FinInstrmGnlAttrbts",{}).get("CmmdtyDerivInd","")
      new_data['NtnlCcy'] = data.get("TermntdRcrd",{}).get("FinInstrmGnlAttrbts",{}).get("NtnlCcy","")
      new_data["Issr"] = data.get("TermntdRcrd",{}).get('Issr',"")
      main_data.append(new_data)
  except Exception as e:
    log.info("key not found error {}".format(e))
    raise("key not found eror")
  df = pd.DataFrame(main_data)
  log.info("data frame shape {}".format(df.shape))
  log.info(df.head(5))
  df.to_csv('parsed_file.csv')


  def upload_file_to_bucket(bucket_name, file_path):
    session = aws_session()
    s3_resource = session.resource('s3')
    file_dir, file_name = os.path.split(file_path)
    bucket = s3_resource.Bucket(bucket_name)
    bucket.upload_file(
      Filename=file_path,
      Key=file_name,
      ExtraArgs={'ACL': 'public-read'}
    )
    s3_url = f"https://{bucket_name}.s3.amazonaws.com/{file_name}"
    return s3_url

s3_url = upload_file_to_bucket('mybucket_name', '/content/parsed_file.csv')
log.info("s3 url {}".format(s3_url))