# cs499-Hadoop
Using Hadoop to find out the highest rated movies and the users who voted the most


#### Input1: TrainingRatings.txt
Format: [MovieID],[User-ID],[1-5 rating]

#### Input2: movie_titles.txt
Format: [MovieID],[Movie Name]

#### Output1: MovieRatings
Format: [MovieID]\t[Average rating]

#### Output1.2: MovieRatingsNames
Format: [MovieID]\t[Average rating]

#### Output1.3: MovieSort
Format: [Rating]\t[Movie ID List Separated by comma]

Sorted from Ratings highest to lowest

#### Output1.4: MovieSortTopTen
Format: [Rating]\t[Movie ID List Separated by comma]

Only top 10 movies

#### Output2: UserCount
Format: [User-ID]\t[Number of votes]

#### Output2.2: UserSort
Format: [Number of votes]\t[User-ID List]

Sorted from highest to lower number of votes

#### Output2.3: UserSortTopTen
Format: [Number of votes]\t[User-ID List]

Only top 10 User ID is printed
