#-------------------------------------------------------------------------
# AUTHOR: Youstina Gerges
# FILENAME: db_connection_solution.py
# SPECIFICATION: Creates, deletes, updates documents, creates categories, and inverts indexes based on User's inputs
# FOR: CS 4250- Assignment #2
# TIME SPENT: 2 days
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
import psycopg2
from psycopg2.extras import RealDictCursor


def connectDataBase():
    # Create a database connection object using psycopg2
    DB_NAME = "DOC"
    DB_USER = "postgres"
    DB_PASS = "6699"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn
    

    except:
        print("Database not connected successfully")
        
def createTables(cur, conn):
    try:
        #Create table
        # create_Categories = "CREATE table categories (id integer NOT NULL, " \
        # " name character varying(225) NOT NULL, "\
        # "constraint Categories_pk primary key (id))"
        create_Categories = "CREATE TABLE categories (id SERIAL PRIMARY KEY, name VARCHAR(225) NOT NULL);"
        
        create_Documents = "CREATE table Documents (id integer NOT NULL, text VARCHAR(225) NOT NULL, " \
                        "title VARCHAR(225) NOT NULL, " \
                        "num_chars integer NOT NULL, " \
                        "date date NOT NULL, " \
                        "category VARCHAR(255) NOT NULL, " \
                        "constraint Documents_pk primary key (id))"

        create_TERMS = "CREATE table Terms (term VARCHAR(50) PRIMARY KEY, term varchar NOT NULL, num_chars integer NOT NULL);"

        create_indexing = "CREATE TABLE Indexing (doc_id SERIAL PRIMARY KEY, term varchar NOT NULL, term_count integer NOT NULL)" #doc_id -> doc.id; term -> list of words from doc.id ; term_count -> count of same word from doc.id
        
        cur = conn.cursor()
        cur.execute(create_Categories) #creates Categories
        cur.execute(create_Documents) #creates Documents
        cur.execute(create_TERMS) #creates Terms
        cur.execute(create_indexing) #creates indexing
        
        cur.connection.commit()
    except:
        conn.rollback()
        print("There was a problem during the database creation or the database already exists.")

def createCategory(cur, catId, catName):

    # Insert a category in the database
    sql= "INSERT INTO categories (id, name) VALUES (%s,%s);"

    recset = (catId, catName)
    cur.execute(sql, recset)
    cur.connection.commit()


def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    cat_id_sql = "SELECT id FROM Categories WHERE name = %s"
    cur.execute(cat_id_sql, (docCat, )) #Category
    category_id = cur.fetchone()
    
    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    # sql = "Insert into Documents (id, text, title, num_chars, date, category) Values (%s,%s,%s,%s,%s,%s)" #Documents

    if not category_id:
        print("Error: not a category")
        quit()

    #calculate num_chars
    num_chars1 = len([char for char in docText if char.isalpha()]) #removes spaces/punctions
    #executes Document table
    # recset = [docId, docText, docTitle, num_chars, docDate, docCat]
    # cur.execute(sql, recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    words_sql = "SELECT regexp_split_to_table(text, E'\\s+') AS words FROM Documents WHERE id = %s"
    cur.execute(words_sql, (docId, ))
    words_result = cur.fetchall()

    #Changes to Lowercase and removes punctuation
    Words = [''.join(char.lower() for char in str(word['words']) if str(char).isalpha()) for word in words_result]
    print("Terms in document", Words)

    
    print("Terms in document", Words) #words from documents

    # #Changes to Lowercase and removes punctuation
    # terms1 = [''.join(char for char in word.lower() if char.isalpha()) for word in Words]

    insert_docData_sql = "INSERT INTO Documents (id, text, title, num_chars, date, category) SELECT %s, %s, %s, %s, %s, %s FROM Categories WHERE Categories.name = %s;"

    recset3 = (docId, docText, docTitle, num_chars1, docDate, docCat,docCat)
    cur.execute(insert_docData_sql, (recset3))
    cur.connection.commit()


    # 3.2 For each term identified, check if the term already exists in the database
    term_sql = "SELECT term FROM Terms" #gets term column from Terms table
    cur.execute(term_sql)
    existing_terms = (cur.fetchall()) #fetchs all the words from term column of Terms table

    for term2 in Words:
    # 3.3 In case the term does not exist, insert it into the database
        if term2 not in existing_terms:
            insert_term_sql = "INSERT INTO Terms (term) VALUES (%s)" #Terms
            cur.execute(insert_term_sql, term2)

    print("All existing terms", existing_terms)
    #Commit the changes
    cur.connection.commit()

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    # 4.2 Create a data structure the stores how many times (count) each term appears in the document
    freq = {}
    collection = []
    for w in Words:
        if w not in collection:
            collection.append(w)
            freq[w] = 1
        else:
            freq[w] += 1
    
    # print("frequencies:", freq)
    # 4.3 Insert the term and its corresponding count into the database
    #indexing table: doc_id -> doc.id; term -> list of words from doc.id ; term_count -> count of same word from doc.id
    indexing_sql_Ins = "INSERT INTO Indexing (doc_id, term, term_count) VALUES (%s,%s,%s)"
    for k,v in freq.items(): #k -> word and v -> count
        recset = [docId, k,v] #Can have multiple docId with different words and it's freq. For example, doc_id = 1 -> Months : 3, doc_id = 1 -> Summers : 2. How do I have multiple docId in Indexing?
        cur.execute(indexing_sql_Ins, (recset, ))


def deleteDocument(cur, docId):

    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    #create_indexing = "CREATE TABLE Indexing (doc_id integer NOT NULL, term varchar NOT NULL, term_count integer NOT NULL)" #doc_id -> doc.id; term -> list of words from doc.id ; term_count -> count of same word from doc.id

    delete_DocID_sql = "DELETE FROM Indexing WHERE doc_id=%s"
    cur.execute(delete_DocID_sql, (docId))
    cur.connection.commit()
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    # try to see if the words within docId are in other docId. If yes -> don't delete words from Terms Table, else -> delete words from Terms Table
    words_sql = "SELECT regexp_split_to_table(text, E'\\s+') AS words FROM Documents WHERE id = %s"
    cur.execute(words_sql, (docId))
    words_result = cur.fetchall()

    Words = [word for row in words_result for word in row['words'].split()]
    
    #Changes to Lowercase and removes punctuation
    terms_docID = [''.join(char for char in word.lower() if char.isalpha()) for word in Words] #List of words from docId

    other_words_sql = "SELECT regexp_split_to_table(text, E'\\s+') AS Other FROM Documents WHERE id <> %s"
    cur.execute(other_words_sql, (other_words_sql))
    other_words_result = cur.fetchall()

    other_Words = [word for row in other_words_result for word in row['Other'].split()]

    other_terms_otherDocId = [''.join(char for char in word.lower() if char.isalpha()) for word in other_Words] #List of words from other docId

    for i in terms_docID:
        if i not in other_terms_otherDocId:
            delete_term_sql = "DELETE term FROM Terms where term = %s"
            cur.execute(delete_term_sql, (i))

    # 2 Delete the document from the database
    delete_Document_sql = "DELETE FROM Documents WHERE id=%s"
    cur.execute(delete_Document_sql, docId)
    cur.connection.commit()

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Delete the document
    deleteDocument(cur,docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    #id, text, title, num_chars, date, category
    #create_indexing = "CREATE TABLE Indexing (doc_id integer NOT NULL, term varchar NOT NULL, term_count integer NOT NULL)" 
    #doc_id -> doc.id; term -> list of words from doc.id ; term_count -> count of same word from doc.id
    index_sql ="SELECT d.id AS document_id, d.title, i.doc_id, i.term, i.term_count FROM Documents d JOIN Indexing i ON d.id = i.doc_id JOIN Terms t ON i.term = t.term;"
    cur.execute(index_sql)
    query = cur.fetchall()
    print(query)


# freq_Words_sql = "SELECT words, COUNT(*) AS word_count" \
# "FROM ( " \
#     "SELECT LOWER(regexp_split_to_table(regexp_replace(text, '[^\w\s]', '', 'g'), E'\\s+')) AS words "\
#     "FROM Documents "\
# ") AS subquery "\
# "GROUP BY words; "