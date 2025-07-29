import pandas as pd
from sqlalchemy import create_engine

df=pd.read_csv(r"C:\Users\kamal\OneDrive\Documents\Desktop\python project\pandas\social_media.csv")

df['Total_Engagement']=df[['likes','comments','shares']].sum(axis=1)

df['post_time']=pd.to_datetime(df['post_time'])


df['post_hour']=df['post_time'].dt.hour

df['Engagement_Rate']=df['Total_Engagement']/1000

df['Average_engagement_rate']=df.groupby('post_type')['Engagement_Rate'].transform('mean')


highest_day=df.groupby('post_day')['Total_Engagement'].mean().sort_values(ascending=False)
print(highest_day)
top5_post=df.groupby('post_type')['Total_Engagement'].sum().sort_values(ascending=False)
print(top5_post)

def get_hour(hour):
    if 5<=hour<12:
        return "morning"
    elif 12<=hour<17:
        return "Afternoon"
    elif 17<=hour<21:
        return "Evening"
    else:
        return "Night"
    
df['timing']=df['post_hour'].apply(get_hour)
engagement_time=df.groupby('timing')['Total_Engagement'].mean().sort_values(ascending=False)
print(engagement_time)

summary_post=df.groupby('post_type')[['likes','shares','comments','Total_Engagement']].mean().reset_index()
print(summary_post)
summary_day=df.groupby('timing')[['likes','shares','comments','Total_Engagement']].mean().reset_index()
print(summary_day)
summary_platform_type=df.groupby(['platform','post_type'])[['likes','shares','comments','Total_Engagement']].mean().reset_index()
print(summary_platform_type)
summary_post.to_csv( r"C:\Users\kamal\OneDrive\Documents\Desktop\python project\summary_day.csv",index=False)
avg_engagement=df.groupby('post_type')['Total_Engagement'].mean()
total_engagement=df.groupby('timing')['Total_Engagement'].sum()

df['Engagement_sentiment']=df.groupby('sentiment_score')['Total_Engagement'].transform('mean')

print(df)
sentiment={'positive':1,'negative':-1,'neutral':0}
df['sentiment_score_numeric']=df['sentiment_score'].map(sentiment)
print("correlation")
print(df['sentiment_score_numeric'].corr(df['Total_Engagement']))

engine=create_engine("mysql+mysqlconnector://root:keerthi%400330kamalesh@localhost/social")

df.to_sql("cleaned_social",con=engine,if_exists='replace',index=False)

total_eng=pd.read_sql_query("select Total_Engagement from cleaned_social",engine)
print(total_eng)