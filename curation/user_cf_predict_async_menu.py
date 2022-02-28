import os, sys
import pandas as pd
from tqdm import tqdm
import time
import asyncio
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# db management libraries
from dbconfig import Upsert, reduce_mem_usage
from controller import MysqlController

# similarity, prediction
from surprise import Reader, Dataset
from surprise import SVD
from surprise import accuracy
from surprise.model_selection import train_test_split

class UpdateRecommend():
    def __init__(self, file = None):
        if not file:
            _id = input("input id(root) : ")
            _pw = input("input pw       : ")
            _db = input("databases      : ")
            connect_info = ("localhost", 3306, _id, _pw, _db)
        else:
            with open(os.path.join(sys.path[0], file), "r") as f:
                connect_info = list(map(lambda x: x.strip(), f.read().split(",")))
        self.controller = MysqlController(*connect_info)

    def get_user(self):
        # 리뷰를 작성한 유저 목록
        q = """
                SELECT DISTINCT ui.user_id, ui.lat, ui.lng
                FROM user_info ui, reviews r
                WHERE ui.user_id = r.user_id
            """

        DF = pd.read_sql(q, self.controller.conn)
        return reduce_mem_usage(DF)

    def get_total_dataframe(self):

        q2 = """
        SELECT r.user_id, r.menu_id, r.restaurant_id, AVG(like_dislike) as predict
        FROM reviews r
        GROUP BY r.user_id, r.restaurant_id, r.menu_id;
        """ 

        DF = pd.read_sql(q2, self.controller.conn)
        DF = reduce_mem_usage(DF)
        return DF

    async def _user_CF(self):
        df_user = self.get_user()

        proc = [asyncio.ensure_future(self.get_user_data(df_user.iloc[i])) for i in range(0, len(df_user))]
        await asyncio.gather(*proc)

    async def get_user_data(self,df_user):
        radius = 3
        lat = df_user['lat']
        lng = df_user['lng']
        uid = int(df_user['user_id'])

        df = self.get_dataframe(uid, lat, lng, radius)        

        #menu_id
        df_g = df.groupby(['restaurant_id','menu_id']).apply(lambda x: (sum(x['predict']*x['expect_rate']))/sum(x['expect_rate']))
        df_g = df_g.reset_index(name='predict')
        df_g=df_g.groupby('restaurant_id')['predict'].mean()
        df=df_g.reset_index(name='predict')
        df['user_id'] = uid


        for i in tqdm(range(0, len(df), 100000)):
            Upsert(user.controller, table_name='user_predict_2', line = df[i:i+100000], update = 'predict')

    def get_dataframe(self, uid, lat, lng, radius):
        # 사용자 근방의 가게 정보를 가져옴
        # q = """
        # SELECT ri.restaurant_id,
        # ( 6371 * acos( cos( radians(%s) ) * cos( radians( `lat` ) ) * cos( radians( `lng` ) - radians(%s) ) + sin( radians(%s) ) * sin( radians( `lat` ) ) ) ) AS distance
        # FROM restaurant_info ri
        # HAVING distance <= %s
        # """ % (lat, lng, lat, radius)

        q=f"""
        select * from restaurant_info ri 
        where earth_distance(ll_to_earth({lat}, {lng}), ll_to_earth(ri.lat , ri.lng)) < {radius};
        """

        # 특정유저 반경 3km 이내의 리뷰
        q2 = """
        SELECT r.user_id, r.menu_id, r.restaurant_id, AVG(like_dislike) as predict
        FROM reviews r, (%s) q
        WHERE r.restaurant_id = q.restaurant_id 
        GROUP BY r.user_id, r.restaurant_id, r.menu_id
        """ % q

        q3="""
        SELECT q.menu_id, q.restaurant_id, q.user_id, q.predict, uc.expect_rate
        FROM (%s) q, user_comp uc
        WHERE uc.user_id=%s and uc.target_user_id=q.user_id  and uc.expect_rate > 0
        # """%(q2,uid)

        DF = pd.read_sql(q3, self.controller.conn)
        DF = reduce_mem_usage(DF)
        return DF

    def make_recommendation(self, algo, df, uid):
        recomm_df = pd.DataFrame(df[uid], columns=[uid])

        # 위에서 만든 [review_id, menu_id]를 받아서 이에 대한 예상 점수 계산
        # print(recomm_df)
        temp_li = [algo.predict(uid, row.Index[0]).est for row in recomm_df.itertuples()]
        # print(temp_li)
        recomm_df[uid] = temp_li
        # print(df)
        recomm_df = pd.DataFrame(recomm_df.stack())
        recomm_df.index.names = ['menu_id','restaurant_id','user_id']
        recomm_df = recomm_df.reset_index()
        recomm_df.columns = ['menu_id','restaurant_id','user_id','predict']
        recomm_df=recomm_df.reindex(columns=['user_id','menu_id','restaurant_id','predict'])
        # print(recomm_df)
        return recomm_df
    

if __name__ == '__main__':
    start=time.time()
    user = UpdateRecommend(file = "../connection_rds.txt")
    user.controller._connection_info()

    # daily update
    loop = asyncio.get_event_loop()
    loop.run_until_complete(user._user_CF())
    loop.close()

    user.controller.curs.close()
    print('time_final:',time.time()-start)