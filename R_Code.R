# 1. Initialize spark session

# Load SparkR
library(SparkR);

# Initiating the spark session
sc <- sparkR.session(master='local');

#Create a Spark DataFrame
# reading jason files from S3 bucket

CD_Vinyl <- SparkR::read.df("s3://casestudy-atul/reviews_CDs_and_Vinyl_5.json", header=T, "json");
Movies_TV <- SparkR::read.df("s3://casestudy-atul/reviews_Movies_and_TV_5.json", header=T, "json");
Kindle_Store <- SparkR::read.df("s3://casestudy-atul/reviews_Kindle_Store_5.json", header=T, "json");



# examine the sizes
nrow(CD_Vinyl)
## 1097592
nrow(Movies_TV)
## 1697533
nrow(Kindle_Store)
## 982619


## Considering only those records with more than 10 helpful votes
createOrReplaceTempView(CD_Vinyl, "amazon_reviews_CD_Vinyl")
CD_Vinyl_df <- SparkR::sql("select asin,helpful[0] as useful_vote, helpful[1] as total_vote,overall,reviewText,reviewTime,reviewerID,reviewerName,summary,unixReviewTime, length(reviewText) as ReviewLength ,((helpful[0] / helpful[1])*100  ) as helpfulness from amazon_reviews_CD_Vinyl where helpful[1]>10");

createOrReplaceTempView(Movies_TV, "amazon_reviews_Movies_TV")
Movies_TV_df <- SparkR::sql("select asin,helpful[0] as useful_vote, helpful[1] as total_vote,overall,reviewText,reviewTime,reviewerID,reviewerName,summary,unixReviewTime, length(reviewText) as ReviewLength ,((helpful[0] / helpful[1])*100  ) as helpfulness from amazon_reviews_Movies_TV where helpful[1]>10");

createOrReplaceTempView(Kindle_Store, "amazon_reviews_Kindle_Store")
Kindle_Store_df <- SparkR::sql("select asin,helpful[0] as useful_vote, helpful[1] as total_vote,overall,reviewText,reviewTime,reviewerID,reviewerName,summary,unixReviewTime, length(reviewText) as ReviewLength ,((helpful[0] / helpful[1])*100  ) as helpfulness from amazon_reviews_Kindle_Store where helpful[1]>10");

##############################
## Collecting data in memory

kindle_reviewer_count <- collect(select(Kindle_Store_df, Kindle_Store_df$asin, Kindle_Store_df$reviewerID,Kindle_Store_df$overall,Kindle_Store_df$ReviewLength,Kindle_Store_df$helpfulness));
head(kindle_reviewer_count);

cd_vinyl_count <- collect(select(CD_Vinyl_df,CD_Vinyl_df$reviewerID,CD_Vinyl_df$overall,CD_Vinyl_df$ReviewLength,CD_Vinyl_df$helpfulness))
head(cd_vinyl_count)

movies_tv_reviewer_count <- collect(select(Movies_TV_df,Movies_TV_df$reviewerID,Movies_TV_df$overall,Movies_TV_df$ReviewLength,Movies_TV_df$helpfulness))
head(movies_tv_reviewer_count)

## Calculating the  Market Percantage by taking the number of  
## distinct reviewers as proxy of buyers

kindle_reviewers <- length(unique(kindle_reviewer_count$reviewerID))
kindle_reviewers
##10691

movies_reviewers <- length(unique(movies_tv_reviewer_count$reviewerID))
## 43989

cd_reviewers <- length(unique(cd_vinyl_count$reviewerID))
## 34012

##Share %age
kindle_share_percentage <- (kindle_reviewers / (kindle_reviewers + movies_reviewers + cd_reviewers))*100
kindle_share_percentage
##12.05%

movies_percentage <- (movies_reviewers / (kindle_reviewers + movies_reviewers + cd_reviewers))*100
movies_percentage
## 49.59748

cd_percentage <- (cd_reviewers / (kindle_reviewers + movies_reviewers + cd_reviewers))*100
cd_percentage
## 38.34844

### Thus it shows max percentage of market share is taken by Movies & TV category followed by CD_Vinyl


##Plotting a pie chart to show the percentage market  share for the 3 categories
slices <- c(kindle_share, cd_vinyl_share, movies_tv_share)
labels <- c("Kindle", "CD and Vinyl", "Movies TV Share")
pie(slices, labels = labels, main = "Market share for Categories");

library(ggplot2)
install.packages("cowplot")
library(cowplot);


########## Rating distribution across categories #############

##Comparing the ratings across all the 3 different categories

plot_grid(
  ggplot(kindle_reviewer_count, aes(overall))+ geom_histogram(binwidth= 1) +
    scale_x_continuous(breaks=c(seq(1,5,by=1))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("Kindle"),
  ggplot(cd_vinyl_count, aes(overall))+ geom_histogram(binwidth = 1) +
    scale_x_continuous(breaks=c(seq(1,5,by=1))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("CD and Vinyl"),
  ggplot(movies_tv_reviewer_count, aes(overall))+ geom_histogram(binwidth = 1) +
    scale_x_continuous(breaks=c(seq(1,5,by=1))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("Movies and TV")    
);

## The above plotting shows the results are best for Kindle as it has maximum propotion with rating 5. It is followed by CD and Vinly and then Movies and TV

##Comapring the mean values of rating  for each of the 3 categories

## Kindle
summary(kindle_reviewer_count$overall);
## Mean 3.76

## CD and Vinyl
summary(cd_vinyl_count$overall);
## Mean 3.67

## Movies and TV
summary(movies_tv_reviewer_count$overall);
## Mean 3.36

## The above analysis shows that the Category with the best rating is Kindle followed by CD and Vinyl and then by Movies and TV


######### Reivew length distribution ###########
plot_grid(
  ggplot(kindle_reviewer_count, aes(ReviewLength))+ geom_histogram() +
    scale_x_continuous(breaks=c(seq(100,4500,by=500),seq(5000,15000,by=3000))) + ggtitle("Kindle") +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)),
  ggplot(cd_vinyl_count, aes(ReviewLength))+ geom_histogram() +
    scale_x_continuous(breaks=c(seq(100,4500,by=800),seq(5000,15000,by=3000))) + ggtitle("CD and Vinly") +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)),
  ggplot(movies_tv_reviewer_count, aes(ReviewLength))+ geom_histogram() +
    scale_x_continuous(breaks=c(seq(100,4500,by=800),seq(5000,15000,by=3000))) + ggtitle("Movies and TV") +
    theme(axis.text.x = element_text(angle = 90, hjust = 1))
  
);

summary(kindle_reviewer_count$ReviewLength)
## 1134
summary(movies_tv_reviewer_count$ReviewLength)
##Mean 1684
summary(cd_vinyl_count$ReviewLength)
##Mean 1544    

## Graph shows maximum people have written between 350 - 850 words for Kindles
## Kindle Reviewers on an average have written around 1134 characters & max being 17830 characters
## Other categories compared to kindle have higher number of shorter reviews ie less than 500

##Finding out the  percentage of reviews with lengths > 500 and lengths < 500
Kindle_less_than_500 <- (nrow(kindle_reviewer_count[kindle_reviewer_count$ReviewLength < 500,])/ nrow(kindle_reviewer_count))*100;
Kindle_less_than_500; --30.89
Kindle_greater_than_500 <- 100- Kindle_less_than_500
Kindle_greater_than_500; --69.10


CD_and_Vinyl_less_than_500 <- (nrow(cd_vinyl_count[cd_vinyl_count$ReviewLength < 500,])/ nrow(cd_vinyl_count))*100;
CD_and_Vinyl_less_than_500; --17.95
CD_and_Vinyl_greater_than_500 <- 100- CD_and_Vinyl_less_than_500
CD_and_Vinyl_greater_than_500; --82.05

Movies_and_TV_less_than_500 <- (nrow(movies_tv_reviewer_count[movies_tv_reviewer_count$ReviewLength < 500,])/ nrow(movies_tv_reviewer_count))*100;
Movies_and_TV_less_than_500; --19.47
Movies_and_TV_greater_than_500 <- 100- Movies_and_TV_less_than_500
Movies_and_TV_greater_than_500; --80.52



######### Helpfulness score distribution ###########

plot_grid(
  ggplot(kindle_reviewer_count, aes(helpfulness))+ geom_histogram(binwidth= 20) +
    scale_x_continuous(breaks=c(seq(0,100,by=20))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("Kindle"),
  ggplot(cd_vinyl_count, aes(helpfulness))+ geom_histogram(binwidth = 20) +
    scale_x_continuous(breaks=c(seq(0,100,by=20))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("CD and Vinyl"),
  ggplot(movies_tv_reviewer_count, aes(helpfulness))+ geom_histogram(binwidth = 20) +
    scale_x_continuous(breaks=c(seq(0,100,by=20))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("Movies and TV")    
);

## The above plot shows that the best helpfulness distribution is for Kindle with maximum difference in the heights of the 2 bins of 80-100 and 0-20

##Checking the mean helpfulness for each of the 3 categories

## Kindle
summary(kindle_reviewer_count$helpfulness);
## Mean 83.37

## CD and Vinyl
summary(cd_vinyl_count$helpfulness);
## Mean 69.75

## Movies and TV
summary(movies_tv_reviewer_count$helpfulness);
## Mean 65.14

## The above analysis shows that the category - Kindle has the best helpfulness score


############ Variation of helpfulness with Rating #############

plot_grid(
  ggplot(kindle_reviewer_count, aes(y=overall,x=helpfulness))+ geom_smooth() + scale_y_continuous(breaks=c(seq(1,5,by=1))) + ggtitle("Kindle"),
  ggplot(cd_vinyl_count, aes(y=overall,x=helpfulness))+ geom_smooth() + scale_y_continuous(breaks=c(seq(1,5,by=1))) + ggtitle("CD and Vinly"),
  ggplot(movies_tv_reviewer_count, aes(y=overall,x=helpfulness))+ geom_smooth() + scale_y_continuous(breaks=c(seq(1,5,by=1))) + ggtitle("Movies and TV")    
);

##The above plot  shows that better the helpful score of the product, the better is the rating 
##The products with 4 or 5 stars rating had reviews which were mostly considered helpful across all categories

########## Variation of helpfullness with Review Length ###########

plot_grid(
  ggplot(kindle_reviewer_count, aes(y=ReviewLength,x=helpfulness))+ geom_smooth() + ggtitle("Kindle"),
  ggplot(cd_vinyl_count, aes(y=ReviewLength,x=helpfulness))+ geom_smooth()  + ggtitle("CD and Vinly"),
  ggplot(movies_tv_reviewer_count, aes(y=ReviewLength,x=helpfulness))+ geom_smooth()  + ggtitle("Movies and TV")    
);

##The above plot shows that the reviews with larger lengths are the ones that correspond to better helpful score
## From above graphs we can conclude that user who is happy with product will write passional helpful reviews which will benefit other users. 
##Thus product category with high average rating and most helpful reviews might make their buyer happy and likely to be purchased heavily

########### Customer Satisfaction - plottig the helpfulness where review lengths are high #############

plot_grid(
  ggplot(kindle_reviewer_count[kindle_reviewer_count$ReviewLength> 2000,], aes(helpfulness))+ geom_histogram(binwidth= 20) +
    scale_x_continuous(breaks=c(seq(0,100,by=20))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("Kindle"),
  ggplot(cd_vinyl_count[cd_vinyl_count$ReviewLength>2000,], aes(helpfulness))+ geom_histogram(binwidth = 20) +
    scale_x_continuous(breaks=c(seq(0,100,by=20))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("CD and Vinyl"),
  ggplot(movies_tv_reviewer_count[movies_tv_reviewer_count$ReviewLength>2000,], aes(helpfulness))+ geom_histogram(binwidth = 20) +
    scale_x_continuous(breaks=c(seq(0,100,by=20))) +
    theme(axis.text.x = element_text(angle = 90, hjust = 1)) + ggtitle("Movies and TV")    
);

## For the above plot, we see a significant higher propotion where the helpfulness score > 0.80-100

##Checking the mean helpfulness for each of the 3 categories when review length > 2000

## Kindle
summary(kindle_reviewer_count[kindle_reviewer_count$ReviewLength>2000,]$helpfulness);
## Mean 87.08

## CD and Vinyl
summary(cd_vinyl_count[cd_vinyl_count$ReviewLength>2000,]$helpfulness);
## Mean 82.80

## Movies and TV
summary(movies_tv_reviewer_count[movies_tv_reviewer_count$ReviewLength>2000,]$helpfulness);
## Mean 76.82

## The above analysis shows that the category - Kindle has the best helpfulness score


########### Variation of ReviewLength with rating ##############

plot_grid(
  ggplot(kindle_reviewer_count, aes(x=ReviewLength,y=overall))+ geom_smooth() + ggtitle("Kindle"),
  ggplot(cd_vinyl_count, aes(x=ReviewLength,y=overall))+ geom_smooth()  + ggtitle("CD and Vinly"),
  ggplot(movies_tv_reviewer_count, aes(x=ReviewLength,y=overall))+ geom_smooth()  + ggtitle("Movies and TV")    
);


### Answering the specific questions
##1) Which product category has a larger market size
#Movies & TV category with 49.59% of market share which is almost half of the market.

##2) Which product category is likely to be purchased heavily

## The category where overall rating have been higher and users have written passionate helpful reviews with highest ratings.
#a) Kindle satisfy all the conditions as most of the reviews have got rating of 4 & 5
#b) Kindle had the most helpful reviews compared to other categories and most helpful reviews were of rating 4 & 5.
#Kindle had least market share thus oppurtunity to grow.
#b) Kindle had the most helpful reviews compared to other categories. 

##3)Which product category is likely to make the customers happy after the purchase
# Category with higher reviews text length with 4-5 rating will make their users happy. As users write lenghty reviews with good ratings if they are happy about the product as
#Above criteria is fulfiled by CD & Vinyl as it has ~82% of the reviews  with more than 500 words which denotes users take time to write reviews for products with higher rating as evident by reviewtext lenght and tating graph


