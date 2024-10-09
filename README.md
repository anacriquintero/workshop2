<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>README - ETL Workshop: Spotify and Grammy Awards Data Integration</title>
</head>
<body>

<h1>ETL Workshop: Spotify and Grammy Awards Data Integration</h1>

<h2>Introduction</h2>
<p>
    This project focuses on exploratory data analysis (EDA) of Grammy Awards and Spotify-related datasets, utilizing modern data science and infrastructure management tools. 
    The Grammy dataset was stored in a PostgreSQL database and processed using Python. Airflow was implemented for workflow orchestration, while Docker ensured environment consistency.
    Additionally, Power BI was used for data visualization, and GitHub for source code management and version control.
</p>

<h2>Project Setup</h2>

<h3>Repository Initialization</h3>
<p>
    The project starts by creating a Git repository in the working directory using the <code>git init</code> command. 
    The directory <code>/home/ana/workshop2/</code> was used to store the project files.
</p>

<h3>Virtual Environment</h3>
<p>
    A Python virtual environment was created using the <code>venv</code> module to manage dependencies in an isolated manner. 
    This ensures there are no conflicts with other projects on the system. The environment was activated with the command:
    <code>source venv/bin/activate</code>.
</p>

<h3>Airflow Setup</h3>
<p>
    Airflow was used for workflow orchestration. The <code>AIRFLOW_HOME</code> environment variable was set to point to the DAGs directory. 
    The services were started in standalone mode to initialize Airflow. Afterward, the Airflow web interface was accessed to verify that the setup was successful.
</p>

<h3>PostgreSQL Setup</h3>
<p>
    PostgreSQL was installed to manage the Grammy Awards dataset. A table named <code>grammys</code> was created to store information such as 
    <em>year</em>, <em>title</em>, <em>category</em>, and <em>artist</em>.
    Docker was also used to containerize the PostgreSQL service, ensuring the database could be accessed consistently across different environments.
</p>

<h2>Technologies Used</h2>
<ul>
    <li>Python</li>
    <li>PostgreSQL</li>
    <li>Docker</li>
    <li>Apache Airflow</li>
    <li>Power BI</li>
    <li>GitHub for version control</li>
    <li>Google Cloud (for Drive integration)</li>
</ul>

<h2>Exploratory Data Analysis (EDA)</h2>

<h3>Spotify Dataset</h3>
<p>
    The exploratory data analysis (EDA) process identified patterns, anomalies, and outliers in the Spotify dataset. 
    Python libraries like Pandas and Matplotlib were used to visualize outliers and generate a correlation matrix. 
    Key findings included strong positive correlations between features such as loudness and energy.
</p>

<h3>Grammy Awards Dataset</h3>
<p>
    EDA on the Grammy Awards dataset involved handling missing values, outlier detection, and generating visualizations of the most frequent 
    artists, songs, and categories. Notable insights included the most frequent nominations for "Song of the Year" and a detailed look at 
    Grammy winners across the years.
</p>

<h2>ETL Process</h2>
<p>
    The integration of data from the Grammy Awards and Spotify datasets was done through an ETL process managed by Airflow DAGs. 
    The DAGs orchestrated data extraction, transformation, and loading into a unified format, preparing it for further analysis.
</p>

<h2>Google Cloud and Drive Integration</h2>
<p>
    Google Cloud was used to connect the project with Google Drive via the Drive API, enabling automated file uploads, downloads, and other file manipulations. 
    The setup involved creating a project in Google Cloud, configuring credentials, and establishing a connection to manage files in the Drive environment.
</p>

<h2>How to Run the Project</h2>
<ol>
    <li>Clone the repository: <code>git clone https://github.com/your-repo/workshop2.git</code></li>
    <li>Create and activate the virtual environment: <code>python3 -m venv venv && source venv/bin/activate</code></li>
    <li>Install dependencies: <code>pip install -r requirements.txt</code></li>
    <li>Set up Airflow: Define the <code>AIRFLOW_HOME</code> variable and start Airflow services: <code>airflow standalone</code></li>
    <li>Set up and run PostgreSQL within a Docker container: <code>docker-compose up -d</code></li>
    <li>Execute DAGs in Airflow to initiate the ETL process for Spotify and Grammy Awards data.</li>
</ol>

</body>
</html>
