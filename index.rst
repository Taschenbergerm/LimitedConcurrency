Limited Concurrency
######################

:date: 2020-09-27 20:40
:modified: 2020-09-27 20:40
:tags: Concurrency, Python, Multiprocessing, Pool
:category: Concurrency
:slug: limited-concurrency
:authors: Marvin Taschenberger
:summary:

Concurrency is a relatively advanced topic in programming and especially in Python.

On of my first task at my Company was to rewrite a non-functioning ETL-Service  which grabs data from a REST API by
one of our vendors, checks and transform the  data and finaly stages a file for the database ingestion. The tricky part was that:

    #. The number of total requests was several hundreds
    #. Only 50 concurrent requests were allowed by the venodr
    #. Timing and retires where critical
    #. Even though downloading the data took the most time, transformation and staging were significant too

Here we are going to simulate this problem with a pokemon-api. We will try to download the 151 pokemon from the REST Api and ingest them into a DB (sqlite).
Through out we will add constraint by constraint and improve the software. For this simple example we will use ``requests`` for the Http calls,
``sqlite3`` for the database and ``loguru`` for logging purpose. As written in my other Blog `a link relative to the current file <{filename}../../stop_printing_start_logging/index.rst>`_
logging is quite important for every process. This is even more important for multiprocessing! Otherwise you might never figure out which of your processes is actually still running and which is done, stuck or dead - trust me logging is essential.


Before we start with the class that contains the actual business logic - lets start with to helpers that we should use

1. A dataclass for the pokemon data stat we are interested in:

.. include:: examples/pokemon.py
    :code: python

2. A function to create a clean database

.. include:: examples/database.py
    :code: python



So lets start by creating a ETL-Class that does exatly the states as defines before

    #. some utilities such as a ``prepare`` function to load a token to authorize ourself on the API and for logging and a ``main`` to glue the logic together
    #. a ``load`` function to download the required data
    #. a ``transform`` function to change the structure of the data - for this example we will also add some ``sleep`` to artifically increase the time each process has to spend within the function
    #. and finally an ``export`` function to write the data into a database.

**Note** - For the PokeApi you actually do not need to log in - though we will include this part for correctness and
we will use it later


Here is what a naive example would look like:

.. include:: examples/starting_point.py
    :code: python


As you can see the example is straight forward as the ``main`` glues together the ``prepare`` function to retrieve
the (pseudo) token for the api and continue by iteratively load, transform and export each  pokemon.
Now we will start to refactor our code to include the first Constraint - the time.
Dealing with every pokemon sequentially takes a huge load of time and doesn't really take advantage of the machine that we have.
To deliver the data in time we need to use ``multiprocessing`` and start treating the request individually.
For that we need to change two things - first wrap the ``load``, ``transform`` and ``export`` into a wrapper thats chains them
and second (naivly again) tranform the for loop into a loop that creates processes to execute the body.

.. include:: examples/example_1.py
    :code: python
    :start-line: 20
    :end-line: 37

So the wrapper does nothing more that chain the body of the for loop into one function - no changes to the code itself.
The majority of the changes happened in the ``main`` function where we now create a ``processes`` list (line 25) that contains one processes
for each pokemon and is ready to execute the newly created wrapper. We then start all these processes (line 27 f.)
and then wait for each of them to finish (30 f.). Thus we will start 151 processes and each of them will load , transform and export
one pokemon atomically. This is totally fine even if your machine has way less cores than that - your operating system will
take care of them and assign a core when they can execute. The only problem you might run into is when your start
hundreds / thousands of them as your system will need to open files for each for communication purpose.

But this is were our second constraint is coming in - the vendor does allow for so many concurrent requests!
So we do need to limit it and only allow for **10** at a time. We can easily achieve that by switching our naive process
generation for a **Worker Pool**. As the name might suggest we are creating a predefined number of processes that then
can take work on some job-list. Each worker/process will take a job, work on it and  once it is done it will pick the
next one in the list and so on.  So we will now create a pool with **10** processes to work on our list

.. include:: examples/example_2.py
    :code: python
    :start-line: 20
    :end-line: 27

We can simple use the ``mp.Pool`` as a contextmanager using ``with`` and  ``map`` the function we want to execute
(the ``wrapper``) to an iterable (``range(1,152)``).  Executing this an looking through the logs we can see that now we only
have ten times the message "Process Pokemon X" popping up right at the start as we only have ten workers now.
Nevertheless, our code takes a huge load of time now as we not only load with only **10** processes at a time,
but also transform and export with a maximum of **10** at once - even though we would not be limited here.

So lets try to increase our pool but start to limit the concurrently requesting processes manually. We can achieve this
by letting the process communicate with each other trough a ``mp.Queue``. This a basically like normal queue where we can
``put`` in and ``get`` out an objects - just that we can do that between processes. Here we will create a queue and put only
**10** of our (pseudo) tokens inside. Each worker will need to first pick a token before he can doe the requests.
Afterwards he will return it and anther worker can get it. With this we limit the number of concurrent loads to 10 and extend
our concurrent transforms & exports.

So first lets create this queue by changing our ``prepare``-method:

.. include:: examples/example_3.py
    :code: python
    :start-line: 37
    :end-line: 44

Though normal mp.processes like in our first
extension can use one directly as they inherit, our pool needs a ``mp.Manager`` to watch over the queue.
So we first need to create one which can then give us a queue.
Moreover, we need to adjust our worker pool as their are now not mapping anymore but need to apply a function with more
arguments.

.. include:: examples/example_3.py
    :code: python
    :start-line: 37
    :end-line: 44


As you can see we switch the ``map`` for an ``apply_async``  where we hand the workers the id as and argument and the
new queue as a keyword. Finally  we need to adjust the ``load``-method from earlier to first pull a token and put it back
afterwards. Beware that we need to ensure the returning of the token even when an error happens.

.. include:: examples/example_3.py
    :code: python
    :start-line: 44
    :end-line: 54

As you can see now we will first pick a token (line 46) from the queue with a limited number of those. So even though we
have 24 processes only 10 can get a token at a time and load the data from the API. Once they are done - they return it
and the next process can take the token while it continues to transform the data.


And this is it - we now have an efficient Service that can download while paying respect to the concurrency limit from
the vendor and still execute the later stages efficiently with more processes! And if we look at the code in general we
didn't even need to change it too much and it still stays readable and maintainable. As such this code i still written
for humans first and computers seconds - **as code should be** - while still being performant.