######################################################
# table definition
# - restaurant_info : 식당정보 저장(id : restaurant_id)
# - menu_info : 메뉴 저장(id : menu_id)
# - reviews : 리뷰 데이터 저장(id : _id)
######################################################

from numpy.core.fromnumeric import shape
from pandas.core.frame import DataFrame
from controller import MysqlController
import os
import sys
import numpy as np
import pandas as pd

global restaurant_info
global menu_info
global reviews

def Upsert(controller, table_name : str = None, line = None, update : str = None):
    # ('review_id', 'menu_id', 'menu', 'restaurant_id', 'restaurant', 'predict', 'predict',)
    columns = ",".join(list(line.columns)) # columns = ",".join(line.keys())
    placeholders = ",".join(['%s']*len(list(line.columns))) # placeholders = ",".join(['%s']*len(line))

    # sql_command = f"""INSERT INTO {table_name}
    #                 ({columns}) VALUES({placeholders})
    #                 ON DUPLICATE KEY UPDATE {update} = VALUES({update})"""

    sql_command = f"""INSERT INTO {table_name}
                    ({columns}) VALUES({placeholders})
                    ON CONFLICT (user_id,target_user_id)
                    DO UPDATE SET {update} = excluded.{update}
                    """

    temp = list(map(tuple,line.values))
    del line

    try:
        # print(sql_command)
        controller.curs.executemany(sql_command, temp) # controller.curs.execute(sql_command, tuple(str(val) for val in line.values()))
        controller.conn.commit()
    except Exception as e:
        print(e)
        print(sql_command)


def reduce_mem_usage(props):
    start_mem_usg = props.memory_usage().sum() / 1024**2 
    # print(f"Before Reduce : {start_mem_usg:.3f} MB")
    # null이 없는 column 사용
    # NAlist = []
    for col in props.columns:
        if props[col].dtype != object:  # Exclude strings
            
            # Print current column type
            # print("******************************")
            # print("Column: ",col)
            # print("dtype before: ",props[col].dtype)
            
            # make variables for Int, max and min
            IsInt = False
            mx = props[col].max()
            mn = props[col].min()
            
            # Integer does not support NA, therefore, NA needs to be filled
            if not np.isfinite(props[col]).all(): 
                # NAlist.append(col)
                props[col].fillna(mn-1,inplace=True)  
                   
            # test if column can be converted to an integer
            asint = props[col].fillna(0).astype(np.int64)
            result = (props[col] - asint)
            result = result.sum()
            if result > -0.01 and result < 0.01:
                IsInt = True

            
            # Make Integer/unsigned Integer datatypes
            if IsInt:
                if mn >= 0:
                    if mx < 255:
                        props[col] = props[col].astype(np.uint8)
                    elif mx < 65535:
                        props[col] = props[col].astype(np.uint16)
                    elif mx < 4294967295:
                        props[col] = props[col].astype(np.uint32)
                    else:
                        props[col] = props[col].astype(np.uint64)
                else:
                    if mn > np.iinfo(np.int8).min and mx < np.iinfo(np.int8).max:
                        props[col] = props[col].astype(np.int8)
                    elif mn > np.iinfo(np.int16).min and mx < np.iinfo(np.int16).max:
                        props[col] = props[col].astype(np.int16)
                    elif mn > np.iinfo(np.int32).min and mx < np.iinfo(np.int32).max:
                        props[col] = props[col].astype(np.int32)
                    elif mn > np.iinfo(np.int64).min and mx < np.iinfo(np.int64).max:
                        props[col] = props[col].astype(np.int64)    
            
            # Make float datatypes 32 bit
            else:
                props[col] = props[col].astype(np.float32)
            
            # Print new column type
            # print("dtype after: ", props[col].dtype)
    
    # Print final result
    mem_usg = props.memory_usage().sum() / 1024**2
    # print(f"After Reduce : {mem_usg: .3f} MB ({100*mem_usg/start_mem_usg: .2f}% of the initial size)")
    return props

# def upsert(controller, table_name : str = None, line : dict = None):


if __name__=="__main__":
    with open(os.path.join(sys.path[0],"connection.txt"), "r") as f:
            connect_info = f.read().split(",")
    cont = MysqlController(*connect_info)
    cont._connection_info()

    # table_creation(cont, tformat = restaurant_info)
    # table_creation(cont, tformat = menu_info)
    # table_creation(cont, tformat = reviews)

    # table_creation(cont, tformat = user_predict)
    # table_creation(cont, tformat = user_comp)
    # table_creation(cont, tformat= user_info)

    cont.curs.close()