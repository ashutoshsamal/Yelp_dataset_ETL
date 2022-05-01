import json
import os


with open(os.path.join('/Users/a0s0iro/Desktop/Yelp_dataset_ETL/src/main/resources/data/', 'yelp_academic_dataset_user.json'), 'r',
          encoding='utf-8') as f1:
    ll = [json.loads(line.strip()) for line in f1.readlines()]

    #this is the total length size of the json file
    print(len(ll))

    #in here 2000 means we getting splits of 2000 tweets
    #you can define your own size of split according to your need
    size_of_the_split=1800
    total = len(ll) // size_of_the_split

    #in here you will get the Number of splits
    print(total+1)

    for i in range(total+1):
        json.dump(ll[i * size_of_the_split:(i + 1) * size_of_the_split], open(
            "/Users/a0s0iro/Desktop/Yelp_dataset_ETL/src/main/resources/data/Split_data/user_data/academic_dataset_user_part" + str(i+1) + ".json", 'w',
            encoding='utf8'), ensure_ascii=False, indent=True)