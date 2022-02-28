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

        # 특정유저 반경 3kn 이내의 리뷰

        q2 = """
        SELECT r.user_id, r.restaurant_id, AVG(like_dislike) as predict
        FROM reviews r
        GROUP BY r.user_id, r.restaurant_id;
        """ 

        DF = pd.read_sql(q2, self.controller.conn)
        DF = reduce_mem_usage(DF)
        return DF

    async def _user_SVD(self):
        df_user = self.get_user()
        df_total = self.get_total_dataframe()

        # 점수 범위가 0,1
        reader = Reader(rating_scale = (0, 1))
        # 사용자의 메뉴에 대한 평점이므로, df에서 필요한 부분만 가져와서 데이터셋으로 만들기
        dataset = Dataset.load_from_df(df_total[['user_id', 'restaurant_id', 'predict']], reader)
        # 전체 데이터의 25%를 train set으로 활용
        trainset, testset = train_test_split(dataset, test_size = .25)
        # SVD 알고리즘 적용
        svd = SVD()
        # train
        svd.fit(trainset)
        # testset에 prediction확인
        pred = svd.test(testset)
        #rmse로 측정
        print(accuracy.rmse(pred))
        print("time_svd :", time.time() - start)

        proc = [asyncio.ensure_future(self.get_user_data(svd,df_user.iloc[i])) for i in range(0, len(df_user))]
        await asyncio.gather(*proc)

    async def get_user_data(self,svd, df_user):

        radius = 3
        lat = df_user['lat']
        lng = df_user['lng']
        uid = int(df_user['user_id'])

        df = self.get_dataframe(lat, lng, radius)
        
        df = df.pivot('restaurant_id','user_id','predict')
        
        recomm_df = await loop.run_in_executor(None, self.make_recommendation, svd, df, uid)

        for i in tqdm(range(0, len(recomm_df), 100000)):
            Upsert(user.controller, table_name='user_predict_1', line = recomm_df[i:i+100000], update = 'predict')

    def get_dataframe(self, lat, lng, radius):
        # 사용자 근방의 가게 정보를 가져옴

        q = """
        SELECT ri.restaurant_id,
        ( 6371 * acos( cos( radians(%s) ) * cos( radians( `lat` ) ) * cos( radians( `lng` ) - radians(%s) ) + sin( radians(%s) ) * sin( radians( `lat` ) ) ) ) AS distance
        FROM restaurant_info ri
        HAVING distance <= %s
        """ % (lat, lng, lat, radius)

        # 특정유저 반경 3kn 이내의 리뷰

        q2 = """
        SELECT r.user_id, r.restaurant_id, AVG(like_dislike) as predict
        FROM reviews r, (%s) q
        WHERE r.restaurant_id = q.restaurant_id and r.menu_id != -1 and r.user_id is not null
        GROUP BY r.user_id, r.restaurant_id;
        """ % q

        DF = pd.read_sql(q2, self.controller.conn)
        DF = reduce_mem_usage(DF)
        return DF

    def make_recommendation(self, algo, df, uid):
        recomm_df = pd.DataFrame(df[uid], columns=[uid])

        # 위에서 만든 [review_id, menu_id]를 받아서 이에 대한 예상 점수 계산
        # print(recomm_df)
        temp_li = [algo.predict(uid, row.Index).est for row in recomm_df.itertuples()]

        recomm_df[uid] = temp_li
        recomm_df = pd.DataFrame(recomm_df.stack())
        recomm_df.index.names = ['restaurant_id','user_id']
        recomm_df = recomm_df.reset_index()
        recomm_df.columns = ['restaurant_id','user_id','predict']
        recomm_df=recomm_df.reindex(columns=['user_id','restaurant_id','predict'])
        return recomm_df


if __name__ == '__main__':
    start=time.time()
    user = UpdateRecommend(file = "../connection_rds.txt")
    user.controller._connection_info()

    # daily update
    loop = asyncio.get_event_loop()
    loop.run_until_complete(user._user_SVD())
    loop.close()

    user.controller.curs.close()
    print('time_final:',time.time()-start)