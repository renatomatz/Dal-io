.. _beginners_guide:

Understanding Graphs
====================

What do I mean by "graphical structure"?
----------------------------------------

In a graphical structures data is represented as nodes and operations as edges. Think of it as a way to represent many inter-connected transformations and their input and output data.

Progressive Disclosure of Complexity
------------------------------------ 
The main philosophy behind the graphical structure of Dal-io come from the Deep Learning library Keras. In their documentation, they state that "A core principle of Keras is **progressive disclosure of complexity**. You should always be able to get into lower-level workflows in a gradual way. You shouldn't fall off a cliff if the high-level functionality doesn't exactly match your use case. You should be able to gain more control over the small details while retaining a commensurate amount of high-level convenience."

So you are familiar with Keras, you will understand that they provide users with a plethora of pre-implemented classes (layers and models) that fit into each other, though the user is also free to create subclasses of their own that can be integrated into the Deep Neural Network and interact with it as just another layer.

Likewise, all of the classes described below where made with the objective of being easily customized by more experienced users. After all, the great majority of objects you will be using where implemented like that! Once you feel like you got a hang of Dal-io and want to build your own pieces, check out the `source code <https://github.com/renatomatz/Dal-io>`_ or the :ref:`developers_guide`.


Why is a graphical structure optimal for financial modeling?
------------------------------------------------------------

    * Modern automated financial models retrieve data, clean and dirty, from various sources and through cleaning and integration are able to join them, further process this product and finally derive insights. The problem is that as these models utilize more and more data from various sources, created models tend to become confusing for both technical and non technical people. Also, as there is no unified workflow to deal with these, created models tend to become highly inflexible and lacking portability (onto other models or projects.) A graphical architecture offers an intuitive workflow for working with data, where inputs can have a unified translation, data can be constantly checked for validity and outputs can be used in flexible ways as parts of a bigger system or drive actions.

    * Utilizing large amounts of data can also end up being highly memory-inefficient when data sources are varied and outputs are as simple as a buy/sell command. As in the TensorFlow graphical architecture, using these constructs allow for automatic parallelization of models to better use modern hardware. Applications can also be built to fit multiple models, and updated independently from the rest of the system.

    * Graphs are easy to interpret visually, which is useful for understanding the flow of data and interpreting output or bugs. They are also highly flexible, allowing users to modify pieces or generate new connections while keeping an enforceable system of data integrity.

    * Perhaps most importantly, these graphs are extremely lightweight and portable, which is key for widespread distribution and access. While every piece can be accessed and tested on-the-go for better ease of development, they are ultimately just pieces of a bigger structure, where data flows continuously and leftover data is discarded automatically, keeping the memory and processing burden at a minimum when dealing with massive datasets.


Base Classes
============

These are the classes you will use throughout an analysis, or rather a class that implements their functionality. Getting to know them is important as it makes it easier to identify one when you see one and make it easier to search for one when you don't really remember where to find it.

.. _external:

External <_Node>
----------------

**Manage connections between your environment and an external source.**

Every model requires an origin to the data it uses, and often wants to send this data out again once it's processed. Subclasses of :code:`External` will implement systems to manage the input and output of data to and from an external sources. An external source is any data or application located outside of your python environment. Two common examples are files and graphs. While these can be manipulated from the python environemt, the actual data is stored outside.

:code:`External` class instances will often be redundant with existing connection handlers, but at least subclasses will allow for more integrated connection handling and collection, so that you can have a single supplicant object for each external connection.

As a child class of :code:`_Node`,:code:`External` implements the :code:`.request(**kwargs)` method, which takes in requests and executed valid ones on their external connections.

While this method is responsible for the main requests to and from the data, subclasses will often have other methods to perform more specific actions on it. Additionally, the :code:`**kwargs` parameter will rarely be the same as the one relayed through the :code:`_Transformer.run()` as  :code:`Translator` and :code:`Application` instances will often curate these to be more generalizable to multiple :code:`External` implementations.

**What to Look For:**

    * What the external source is.

    * Is it reliant on configuration? If so, what configuration parameters are required/considered?


_Transformer
------------

**Represent data transformations.**

:code:`_Transformer` instances are defined by their inputs and outputs. IO can be limited to one or more sources and the source can be either internal or external (as defined in :ref:`external`). 

All :code:`_Transformer` instances implement the :code:`.run(**kwargs)` method to:

    #. Request source data from a :code:`_Node` instance.

    #. Apply specific transformations to the sourced data.

    #. Return the transformed data.

This process will vary depending on the subclass, though the one thing to keep in mind is that the output of this method is what will be fed onto the next node on the graph, so it's a powerful tool for debugging.

:code:`_Transformer` instances also define each input in their initialization by using :code:`Validator` instances. You can find more about these in the :ref:`developers-guide` section on the :ref:`validator` but for now, you can use the :code:`_Transformer.describe()` method to get an idea of what kind of inputs this piece requires or prefers.

.. Talk about the input methods and copy

You won't be using these directly in your analyses, but will definitely use one of its subclasses.

**What to Look For:**

    * Number of input and outputs.

    * Sources/destinations of inputs and outputs.

    * Input descriptions.

Translator <_Transformer>
-------------------------

**Request and standardize external data.**

*One external input, one internal output*

While :code:`External` instances are the origin of all data, :code:`Translator` instances are the root of all *clean and standardized* data. Objects of this class have :code:`External` instances as their source and are tasked with creating requests understandable by that instance and standardize the response data into an acceptable format. 

For more information on the Dal-io formatting standards, check out :ref:`formatting`.

All :code:`Translator` instances implement the :code:`.run(**kwargs)` method to:

    #. Source data from an :code:`External` instance.

    #. Translate the data into a format as specified by the formatting guide.

    #. Return the translated data.

These also tend to be the PipeLine stages where :code:`kwargs` source from.

**What to Look For:**

    * Compatible :code:`External` instances.

    * What translation format is being used and how will the output contain.

    * What are the keyword arguments it can interpret.

Pipe <_Transformer>
-------------------

**Transform a single input into a single output.**

*One internal input, one internal output*

Pipes will compose the majority of data wranging and processing in your graphs, and are designed to be easily extendable by users.

All pipes must implement the :code:`.transform(data, **kwargs)` method, which takes in the output from sourced data and returns it transformed. This has three main purposes.

    #. Subclasses can more objectively focus on transforming and outputting the :code:`data` parameter instead of having to deal with sourcing it.

    #. It makes it possible to use :code:`Pipe` instances to transform data outside of the Dal-io library directly, which is useful for applications outside of the library's scope or for testing the transformation.

    #. More efficient compatibility with :ref:`pipeline` objects.

All :code:`Pipe` instances implement the :code:`.run(**kwargs)` method to:

    #. Define input requirements.

    #. Source data from another :code:`_Transformer` instance, applying integrity checks.

    #. Pass it as the :code:`data` parameter to the :code:`.transform()` method.
    #. Return the transformed data.

While the default implementation of the :code:`.run()` method simply sources data and passes into :code:`.transform`, it is often changed to modify keyword arguments passed onto the source node and the .transform() call. 

**What to Look For:**

    * What are the input requirements.

    * What the :code:`.transform` method does.

    * What are changeable attributes that affect the data processing.

Model <_Transformer>
--------------------

**Utilize multiple input sources to get one output.**

*Multiple internal inputs, one internal output*

:code:`Model` instances are a lot like :code:`Pipe` instances as their main task it to transform inputs to get an output. Though taking in multiple inputs might not seem like enough to warrant a whole different class, the key differences come from all the extra considerations needed when creating a :code:`Model` instance. 

There are two main uses for :code:`Model` instances:

    #. Getting multiple inputs and joining them to form a single output.

    #. Using the output of one of the inputs to format a request to another input.

These objectives thus require a lot more flexibility when it comes to sourcing the inputs, which is why, unlike :code:`Pipe` instances, :code:`Model` instances do not have a :code:`.transform()` method, and instead rely solely on their :code:`run()` method to:

    #. Source data from inputs.

    #. Process and transform data.

    #. (Possibly) source more data given the above transformations.

    #. (Possibly) join all sourced data.

    #. Return the final product.

**What to Look For:**

    #. All the input names and what they represent.

    #. The requirements for each input.

    #. How the :code:`.run()` method deals with each input piece.

    #. What changeable attributes affect the data processing.

Application <Model>
-------------------

**Act on external sources**
*Multiple internal inputs, zero or more external or internal outputs*

While you might be using Dal-io mostly for processing data for further use in your python session, :code:`Application` instances offer methods of using this processed data to interact with external sources. These will be managed by :code:`External` instances which are called by the application with data it sources from its inputs. These interactions can take a broad range of forms, from simple printing to the console to graphing, executing trade orders or actively requesting more data from the inputs. Ultimately, :code:`Application` instances offer the greatest set of possibilities for users wanting to implement their own, as it is not bound by the scope of what the library can do.

All :code:`Application` instances implement the :code:`.run(**kwargs)` method to:

    #. Source, validate, process and/or combine data from different inputs.

    #. Use processed input data to send a request to an external source.

    #. Get responses from external sources and further interactions.


**What to Look For:**

    #. All the input names and what they represent.

    #. The requirements for each input.

    #. All the output names and what they represent.

    #. How the :code:`.run()` method deals with each input piece and how will it be transmitted to the output.

Extra Classes and Concepts
==========================

Now that we've seen what will make your models work, lets jump into what will make your models **work incredibly.** 

.. _pipeline:

PipeLine <Pipe>
---------------

As Pipe instances implement a normally small operation and have only one input and one output, you are able to join them together, through the \_\_add\_\_() internal method (which overrides the + operator) to create a sequence of transformations linked one after the other. These simply pass the output of one Pipe instance's .transform() method as the input to another, which can be a significant speed boost, though you should be careful with data integrity here. 

KEEP IN MIND that good alternatives to these is just linking Pipe instances together in order to validate the data at every stage of the pipeline. This will have the same output as a PipeLine, but compromise on speed and possibly aesthetics.

Memory <_Transformer>
---------------------

When using APIs to fetch online data, there is often a delay that ranges from a few to a few dozen seconds. This might be completely fine if data will only pass through your model once to feed an application, for example, but will become a problem if you are also performing analyses on several pieces of the model or have several Model instances in your graph (which call on an input once for every source). The solution to this lies in Memory instances that temporarily save model inputs to some location and retrieves it when ran. 

Notice that Memory inherits from a _Transformer, which makes it compatible as input to any piece of your graph and behaves like any other input (most closely resembling a Pipe.)

Subclasses will implement different storage strategies for different locations. These will have their own data requirements and storage and retrieval logic - imagine the different in data structure, storage and retrieval required for storing data on a database vs on the local python session.

One thing to keep in mind is that these only store one piece of memory, so if you, for example, want to vary your .run() kwargs, this might not be the best option beyond building and debugging your model. If you still want the speed advantages of Memory while allowing for more runtime argument flexibility, check out the LazyRunner class below.

LazyRunner <_Transformer>
-------------------------

These objects are the solution to storing multiple Memory instances for different runtime kwargs that pass through the instance. These do not store the data itself, but rather the memory instances that do. This allows for more flexibility, as any single Memory subclass can be used to store the data. These are created when a new keyword argument is seen, and it does so by getting the data from a _Transformer input and setting its result as the source of a new Memory instance. The Memory type and initialization arguments are all specified in the LazyRunner initialization. 

KEEP IN MIND that these could mean a significant memory burden, if you are widely saving data from different inputs with several kwargs combinations passed on to them.

The solution to the memory problem comes in the buffer= initialization argument of the LazyRunner. These will limit the number of Memory instances that are saved at any point. This also comes with the update= initialization argument for whether or not stored Memory instances should be updated in FIFO order once the buffer is full or whether an error should be thrown.

KEEP IN MIND that this will not notice if its source data input has any sort of input changes itself (this could be a change in date range, for example or data source.) This will become a problem as changes will not be relayed if the runtime kwargs are the same as before a change. This happens as the LazyRunner will assume that nothing changed, see the kwarg and return the (old) saved version of the response. This can be solved by calling the .clear() method to reset the memory dictionary.

Keyword Arguments
-----------------

Just like data propagates forward in the network through nodes and transformers, requests propagate backwards through :code:`.run()` and :code:`request()` keyword arguments. Though often you won't need them (and much less often need to implement a new one), keyword arguments (aka kwargs) are a way on which a front piece of your graph can communicate with pieces before them at runtime. In essence, kwargs are passed from run to request over and over until they reach a node that can use them. These nodes can use these kwargs in different ways.They can:
    
    * Use them to filter sourced data.

    * Use them to create another request, based on previously-unknown information.

Though they might seem like an amazing way of making your graph act more like a function, adding new kwarg requirements should be done very rarely and done with full knowledge of what are the taken kwarg names, as conflicting names will certainly cause several unforeseen bugs.

Tips and Tricks
===============

Importing
---------

As any other python (or any other programming language) workflow, we start with imports. Dal-io will often require several pieces to be used in a workflow, each of which is located within a submodule named after the base classes we have seen above. This means that importing the whole :code:`dalio` package and instantiating piece by piece will often create unappealing code, which is why the following techniques are preferred.

**Import submodules with an alias:**

.. code-block:: python

    import dalio.external as de
    import dalio.translator as dt
    import dalio.pipe as dp
    import dalio.model as dm
    import dalio.application as da

This technique might not be the most standard or space efficient, but is very useful when you are still testing out models and architectures. For most worflows where you want to try out new paths and strategies, having these imports will give you all the core functionality you need while keeping your code clean.

**Import specific pieces from each submodule:**

.. code-block:: python

    from dalio.external import YahooDR, PyPlotGraph

    from dalio.translator import YahooStockTranslator
    
    from dalio.pipe import (
        Change,
        ColSelect,
        Custom,
        DateSelect,
    )

    from dalio.model import (
        OptimumWeights,
        OptimumPortfolio,
    )

    from dalio.application import Grapher

This doing this is more standard to match common workflows like those in :code:`keras` and :code:`sklearn`  though can easily grow out of hand ind a Dal-io workflow, especially when trying to experiment with new inputs and pieces. 

This is preferred once you have created a graph you are happy with and is ready for use. Importing all pieces explicitly not only makes your code more readable, it also makes the used pieces more explicit to the ones reading your implementation.

**A use hybrid approach:**

.. code-block:: python

    from dalio.external import YahooDR, PyPlotGraph
    from dalio.translator import YahooStockTranslator
    
    import dalio.pipe as dp
    import dalio.model as dm

    from dalio.application import Grapher

This approach is a great way of reconciling both importing workflows, as it keeps the most relevant pieces of the graph explicit (the original input, the application and the final output) while giving you flexibility of accessing all :code:`Pipe` and :code:`Model` pieces available for testing.

The Basic Workflow 
------------------

Now that you are familiar with the most common parent classes used in the Dal-io system and ways of importing them, we can start talking about how a basic workflow with them will tend to look like. 

We will separate our basic workflow into the following steps.

    #. Set up imports.

    #. Set up core data sources.

    #. Data wrangling and processing.

    #. Application set-up.

**Set up imports:**

This is the stage where you use set up and configure any :code:`External` object instances and set them as inputs to a Translator. This defines the core of the data that will be sent to the rest of your graph, so it is always positive to have test runs of this raw input.

**Set up core data sources:**

Now that you have your inputs, perform any sort of transformations which will further standardize it to your specific needs. These can be selecting specific columns (like only the "close" column if your source gets OCHLV data) or joining sources.

This is an optional, yet often relevant step, and you should see this as a preparation to the data that will feed every step following this.

If we were to picture a graph with various nodes and edges which source data from a single node, this step is setting up a few nodes between the source and the actual first node that other pieces often get data from. In other words, no other pieces but the ones used in this step will be interacting with the pieces that come before it.

**Data wrangling and processing:**

This is the most general step and is all about setting up processing pipelines for your data. This might involve performing transformations, joining sources into models and maybe even setting up different diagnostic applications midway. Theres no overwhelming structure to these other than setting up the inputs that will feed your last nodes.

**Application set-up:**

While applications are not a requirement for a graph, they are often the very last nodes in one. Above that, :code:`Application` instances often have the largest burden of setup, so deciding all of their pieces and putting together inputs is a common last step. 

Once applications are set up, the following analysis will be for the most part a process of actually using it or optimizing your results by tweaking some of the steps done previously.

When Reading the Docs
---------------------

I find it that reading the docs can be a completely different experience depending on the package I am researching. Whether you want to find out whether a specific process currently exists in the Dal-io library or if you just want to get more specifications on a single piece you know exists, there are a couple of breadcrumbs left as part of the documentation structure that where placed to guide you there.

**Know how your piece fits:**

As you have seen throughout the beginners guide, every Dal-io piece inherits from a base class, which represents a certain state of data or transformation. Knowing well what you are looking for in terms of these states or transformations can go a long way on trying to find the submodule to look for the piece.

You can ask questions like:

    * Is this a transformation on data or a representation of data?

    * How many inputs does this transformation have?

    * Are there any external inputs or outputs involved in this specific piece?

Beyond the base class submodules, these are further organized into different script folders to ensure there is further separation of what the base class implementations do. Definitely see what are the current available submodule "categories" to further narrow your search. The good thing is that while there are separated into links in the :ref:`user-modules` page, they are all joined together into the same specific submodule page.

**Know how to explore your piece:**

Once you have pinpointed your piece, explore its definition or source code to know how to fully utilize it in your specific case. While one could argue that only by going through the source could one fully understand an implementation's full potential, this is often a tedious approach and definitely not beginner-friendly.

If you want to cut to the chase when it comes to knowing a function, look for the things specified under the "What to look for" sessions on each of the base class descriptions above.

Must-Know Classes
-----------------

Now that you are fully armed with the knowledge needed to venture into the package, let's get you introduced to a couple of pieces the development team (currently composed of one) has used with frequently.

.. autoclass:: dalio.pipe.col_generation.Custom
    :members: __init__

The :code:`Custom` pipe does what the name implies: it applies a custom transformation to an input :code:`pandas.DataFrame` instance. It inherits most of its functionality from the :code"`_ColGeneration` abstract class, so reading its description will help you understand how flexible your transformations can be when it comes to reintegrating it back into the original dataframe while keeping its column structure intact.

.. autoclass:: dalio.pipe.col_generation._ColGeneration
    :members: __init__, transform, _gen_cols

This class is extremely important as it essentially the user's first point of entry into creating their custom transformations. :code:`Custom` pipes work by applying your specified function to either the dataframe's rows or columns (specified through the :code"`axis` parameter). 

The application itself is divided into different pandas strategies (specified through the :code"`strategy` parameter, set to :code:`\"apply\"` by default.) The strategies correspond to :code:`pandas.DataFrame` methods, really, so if you want to get to the specifics of its, just read the `pandas documentaion <https://pandas.pydata.org/pandas-docs/stable/reference/index.html>`_ for the :code:`\"apply\"`, :code:`\"transform\"`, :code:`\"agg\"` and :code:`\"pipe\"` descriptions. But for most cases, you will be using two strategies.

    * :code:`\"apply\"`: here, each row or column is passed onto the custom function as :code:`pd.Series` instances. This is the most generic strategy and should used the most often.

    * :code:`\"pipe\"`: unlike :code:`\"apply\"`, here the whole dataframe is passed onto your custom function at once, which can be useful for experimenting with specific functions you might want to implement as a piece later.

.. autoclass:: dalio.pipe.select.DateSelect
    :members: set_start, set_end

This piece also has a name as intuitive as what it does. It essentially takes in a time series :code:`pandas.DataFrame` (one which has a :code:`pandas.DatetimeIndex` as its index) and returns a subset of its dates. What makes it so powerful is its use as a "remote control" for your input time interval.

This effectively gives you an adjustable "filter" that can be adjusted at any point of your analysis to decide what section of the data to perform it on, which is crucial in various kinds of time series analyses. 

For an interesting use case of this, check out the backtesting cookbook!
