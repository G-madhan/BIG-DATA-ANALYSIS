# BIG-DATA-ANALYSIS

*COMPANY*: CODTECH IT SOLUTIONS

*NAME*: MADHAN G

*INTERN ID*: CT08VCD

*DOMAIN*: DATA ANALYTICS

*DURATION*: 4WEEKS

*MENTOR*: NEELA SANTHOSH

##PERFORM ANALYSIS ON A LARGE DATASET USING TOOLS LIKE PYSPARK TO DEMONSTRATE SCALABILITY.

Big data analysis is the process of examining large and complex datasets to uncover hidden patterns, correlations, market trends, customer preferences, and other useful information that can help organizations make more-informed business decisions.

Import Libraries: Imports necessary PySpark modules for data manipulation and analysis.
SparkSession: Creates a SparkSession, which is the entry point for using Spark functionality.

Data Loading: Reads the disney_plus_titles.csv file into a Spark DataFrame (df). header=True indicates the CSV file has a header row, and inferSchema=True tells Spark to automatically determine the data types of the columns.

df.show(5): Displays the first 5 rows of the DataFrame, giving you a quick look at the data.
df.printSchema(): Prints the schema of the DataFrame, showing the column names and their data types.

Fills null values in the specified columns with placeholder values.
Categorical columns (like 'title', 'cast', 'director', etc.) are filled with 'Unknown'.
The 'rating' column is filled with '0'.
This is a basic form of imputation, replacing missing values with default values.

Filtering and Aggregation: Filters the DataFrame to include only movies and calculates the average duration using avg().
Time Conversion: Converts the average runtime from minutes to hours, minutes, and seconds for better readability.
This demonstrates Spark's ability to handle potentially large datasets by using distributed operations such as filter and aggregate.

Splits the comma-separated "cast" column into individual actor names and creates a new row for each actor.
Grouping and Counting: Groups the DataFrame by actor name and counts the number of appearances for each actor.
Ordering and Displaying: Orders the results in descending order of counts and displays the top 10 actors. This demonstrates how spark can process string columns that contain lists of data.

Filtering: Filters the DataFrame to include only movies with non-null directors.
Grouping and Counting: Groups the DataFrame by director and counts the number of movies directed by each director.
Ordering and Displaying: Orders the results in descending order of counts and displays the director movie counts.

Stops the SparkSession, releasing resources.

DATASET: https://www.kaggle.com/datasets/shivamb/disney-movies-and-tv-shows


