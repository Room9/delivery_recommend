import os, sys
import pandas as pd
import asyncio
import time
from tqdm import tqdm
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

# db management libraries
from dbconfig import Upsert, reduce_mem_usage
from controller import MysqlController

class UpdateRecommend():
    def __init__(self, file = None):
        # self.lat = 37.561017
        # self.lng = 126.985802
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

    def get_dataframe(self, lat, lng,radius):
        # MYSQL 기준 GIS query
        # q = """
        # SELECT ri.restaurant_id,
        # ( 6371 * acos( cos( radians(%s) ) * cos( radians( `lat` ) ) * cos( radians( `lng` ) - radians(%s) ) + sin( radians(%s) ) * sin( radians( `lat` ) ) ) ) AS distance
        # FROM restaurant_info ri
        # HAVING distance <= %s
        # """ % (lat, lng, lat, radius)

        # PostgreSQL 기준 GIS query
        q=f"""
        select * from restaurant_info ri 
        where earth_distance(ll_to_earth({lat}, {lng}), ll_to_earth(ri.lat , ri.lng)) < {radius}
        """

        # 특정유저 반경 3km 이내의 리뷰
        q2 = """
        SELECT r.user_id, r.menu_id, r.restaurant_id, AVG(like_dislike) as like_dislike
        FROM reviews r, (%s) q 
        WHERE r.restaurant_id = q.restaurant_id
        GROUP BY r.user_id, r.restaurant_id, r.menu_id;
        """ % q

        DF = pd.read_sql(q2, self.controller.conn)

        DF = reduce_mem_usage(DF)
        return DF

    # 유저간 코사인 유사도 구하기
    def _user_cosine(self, df):
        # 한꺼번에 pivot하면 용량 때문에 종료됨
        rated_df = df.pivot(columns = 'user_id', index = 'menu_id', values = 'like_dislike')
        
        con = rated_df.nunique()==1
        
        rated_df_normal=rated_df.loc[:,~(con)]

        rated_df = rated_df_normal.fillna(rated_df_normal.mean())

        # 열 평균
        df_mean = rated_df.mean()

        # 편차 구하기
        df_deviation = rated_df[df_mean.index] - df_mean

        # 상관계수 구하기
        cos_sim = df_deviation.corr(method='pearson')
        compat_df = pd.DataFrame(cos_sim.unstack())
        del cos_sim
        compat_df.index.names = ['user_id', 'target_user_id']
        compat_df = compat_df.reset_index()
        compat_df.columns = ['user_id', 'target_user_id', 'similarity']

        # 용량 줄이기
        compat_df = reduce_mem_usage(compat_df)

        # User간 궁합점수가 0% ~ 100%로 나오기 때문에 이에 맞게 변환
        compat_df['similarity'] = compat_df['similarity'].apply(lambda x: (x*100))
        return compat_df

    # db update
    ### 유저 상관관계
    async def update_compatibility(self):
        # 리뷰를 작성한 유저 목록
        review_user_df = self.get_user()
        
        proc = [asyncio.ensure_future(self.get_personal_user_data(review_user_df.iloc[i])) for i in range(0, len(review_user_df))]
        await asyncio.gather(*proc)

    async def get_personal_user_data(self, review_user):

        radius = 3000
        lat = review_user['lat']
        lng = review_user['lng']
        uid = review_user['user_id']
# 

        # 1명의 반경 3km 리뷰 목록

        data = self.get_dataframe(lat, lng, radius)

        cos = await loop.run_in_executor(None, self._user_cosine, data)

        cos = cos[cos.user_id == uid]

        Upsert(self.controller, table_name = 'user_comp', line = cos, update='similarity')
        
        del cos



if __name__ == '__main__':

    start = time.time() 
    user = UpdateRecommend(file = "../connection_postgre.txt")
    user.controller._connection_info()
    
    # daily update
    loop = asyncio.get_event_loop()
    loop.run_until_complete(user.update_compatibility())
    loop.close()

    user.controller.curs.close()
    print("time :", time.time() - start)