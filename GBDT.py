import pandas as pd
import numpy as np
import csv

from sklearn.metrics import mean_squared_error, r2_score
from sklearn.ensemble import GradientBoostingRegressor
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler


from sklearn.externals import joblib

import sys
import time

import argparse
import logging

np.random.seed(1)

def add_context(data_df, context = 0):
    L = len(data_df)
    cols = list(data_df.columns)
    context_data = []
    row_1 = np.asarray(data_df.iloc[0,:])
    row_1 = np.reshape(row_1,(1,-1))
    row_1 = pd.DataFrame(row_1, columns=cols)
    #pad data with data[0] context times
    data = pd.concat([row_1]*context + [data_df])
    try:
        del data[0]
    except:
        pass

    data = data.to_numpy()
    for fi in range(context, len(data)):
        frame = data[fi-context:fi+1].flatten()
        context_data.append(frame)
    return pd.DataFrame(context_data, columns = cols*(context+1))

def read_dataset(filename):
    dataset = pd.read_csv(filename)
    return dataset

def get_data_labels(df, context = 0):
    label = df['flow_size']
    #label = df['flow_size']
    try:
      del df['job']
    except:
      pass
    df = add_context(df, context)
    df.iloc[:, -1] = 0
    #add context to labels
    return df, label


def plot_features(labels,values,title = 'Graph',rot = 90):
    index = np.arange(len(labels))
    plt.bar(index*2.0,values,width = 1)
    plt.xticks(index*2.0, labels, fontsize=15, rotation=rot)
    plt.title(title)
    plt.show()
'''
Updated train, test values '''

if __name__ == '__main__':

    start_time = time.time()
    log_format = "%(asctime)s (%(module)s:%(lineno)d) %(levelname)s:%(message)s"
    logging.basicConfig(level=logging.INFO, format=log_format)

    parser = argparse.ArgumentParser()
    parser.add_argument('--task', type=str,
                        choices=['KMeans', 'PageRank','SGD'],
                        help='Type of task. Example: --task KMeans')
    parser.add_argument('--context', type=int, default=1)
    parser.add_argument('--mini', action='store_true',
                        help = 'Make and store predictions on mini test set')
    args = parser.parse_args()

    if args.task is None:
        print('Please enter the name of the task for the test data along with the --task option.\nCheck --help for task')
        sys.exit()

    TEST_PATH = ''
    TEST_NAME = args.task
    #don't need to scale values to the range(0,1)
    #scaler = MinMaxScaler(feature_range=(0,1))

    train_df = read_dataset(TEST_NAME + '_training.csv')
    test_df = read_dataset(TEST_NAME + '_test.csv')
    validation_df= read_dataset(TEST_NAME + '_validation.csv')

    '''
    #scale and convert to df
    #fit on train, transform train, test, validation
    cols = list(train_df.columns)
    train_df = scaler.fit_transform(train_df)
    train_df = pd.DataFrame(train_df, columns = cols)

    test_df = scaler.transform(test_df)
    test_df = pd.DataFrame(test_df, columns = cols)

    validation_df = scaler.transform(validation_df)
    validation_df = pd.DataFrame(validation_df, columns = cols)
    '''

    context = args.context

    train_x, train_y = get_data_labels(train_df, context)
    test_x, test_y = get_data_labels(test_df, context)
    validation_x, validation_y = get_data_labels(validation_df, context)

    if args.task == 'SGD':
        r_s = 4
    elif args.task == 'KMeans':
        r_s = 0
    else:
        r_s = 2

    logging.info('Value of r_s is %d' %r_s)
    regItem = GradientBoostingRegressor( max_depth=10, n_estimators=50, learning_rate=1.0, random_state = r_s)
    regItem.fit(train_x, train_y)
    list_of_err=[]

    for predicted_y in regItem.staged_predict(test_x):
        list_of_err.append(mean_squared_error(test_y, predicted_y))
    topEst = np.argmin(list_of_err)

    bestRegItem = GradientBoostingRegressor( max_depth=7, n_estimators=topEst+1, learning_rate=1.0, random_state = r_s )
    bestRegItem.fit(train_x, train_y)
    #predicted_score = bestRegItem.score(testing_x,testing_y)
    #predicted_y = bestRegItem.predict(testing_x)

    predicted_score = bestRegItem.score(test_x,test_y)
    pred_y = bestRegItem.predict(test_x)

    #print("Accuracy is ",predicted_score)
    print("R2 is ", predicted_score)



    #save best model
    filename = 'gbdt_' + TEST_NAME + '_context' + str(context) + '.pkl'
    logging.info('Saving best GBDT model as %s' %filename)
    joblib.dump(bestRegItem,filename)


    if args.mini:
        #Predict for mini
        logging.info('Predicting for mini')
        mini_filename = TEST_NAME + '_test_jb_mini.csv'
        #load mini test set
        test_mini_df = read_dataset(mini_filename)
        logging.info('Reading mini file from %s' %mini_filename)

        #get original test_df
        test_df['flow_size'] = pred_y


        new_df = pd.DataFrame([])
        new_df['flow_size'] = test_mini_df['flow_size']
        new_df['job'] = test_mini_df['job']
        new_df['predicted_flow_size'] = test_mini_df['flow_size']

        #now copy predicted
        for index, row in test_mini_df.iterrows():
            i = int(row['pseudo_index'])

            #copy over the predicted flow_size to new_df
            #handle negative values
            new_df.iloc[index,2] = max(0.0,test_df.iloc[i]['flow_size'])

        #save
        save_file_name = TEST_NAME + '_test_jb_mini_predicted_GBDT.csv'
        logging.info('Storing to %s' %save_file_name)
        new_df.to_csv(save_file_name)

    end_time = time.time()
    #just to know how much time it takes to train
    logging.info('Time taken to run code: %f seconds' %(end_time - start_time))
