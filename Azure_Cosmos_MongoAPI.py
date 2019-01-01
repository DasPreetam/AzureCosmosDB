#pip install pymongo
import pymongo, ast, glob

try:
    client=pymongo.MongoClient("enter the connection string here")
    db=client['cosmos']
    coll=db['MeterRead']

    path = 'D:/Preetam/MeterRead/Output/*.json'   
    files=glob.glob(path)


    for item in files:
        fileReader = open(item,'r')
        data_raw = fileReader.read()
        fileReader.close()

        upload=coll.insert_one(ast.literal_eval(data_raw))
        print('Upload Successful')

    print('Execution Complete')
    input('Hit Return key to exit')

except BaseException as e:
    print(str(e))
    input("Hit Return to exit")
