# delivery_recommend

배치파일 형태 추천시스템. \
속도 개선을 위한 python async 적용

## user_comp_async.py
- Collabrative Filtering 이용한 유저 간 유사도 산출

## user_cf_predict_async.py
- 유저 간 유사도를 가중치로 menu 추천

## user_predict_async_res.py
- 유저 리뷰데이터와 SVD 이용한 restaurant 추천

## user_predict_async_menu.py
- 유저 리뷰데이터와 SVD 이용한 menu 추천
