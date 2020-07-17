.. _developers_guide:

Core Classes and Concepts
=========================

.. _validator:

Validator
---------

Validators are the building blocks of data integrity in the graph. As modularity is key, validators ensure that data sourced from a DataDef is what it is mean to be or that errors are targeted to make debugging easier. Validators can have any attribute needed, but functionality is stored in the .validate function, which either passes warning data on or stops execution with an error. These can and should be reused with multiple DataDef instanges.

_Node
-----

Node instances represent data. They have a connection to some data input, internal or external, and make requests to this data as well as ensure their integrity. These form the basis for External and DataDef classes.

_DataDef <Node>
---------------

DataDef instances are sources of data and implement mechanisms to ensure the integrity of that data, as input from sources is uncertain.

KEEP IN MIND that this is a tool only usued by developpers while creating new transformations, actual users do not enter in contact with neither Validator nor DataDef instances.

**Validation:** In order to hold descriptions true, the data is validated by a chain of <Validator> functions before returning any actual data, in order to enure that if data is actually returned, it is accurate to its descriptions and won't break the subsequent transformation. These are referred to as descriptions inside DataDef instances and are added to them upon initialization of a Transformer instance.

**Speed Concerns:** While it's understandable that these might pose a significant speed burden to applications, they are designed to reduce these by as much as possible. Firstly, validations are not dependent on each other and can thus be parallelized. Also, they can be turned off as needed, thoug this must be done with caution.

_Builder
--------

Builders are a solution to the problem of standardizing several package workflows into something more consistent to the inexperienced user. 

Take the MakeARCH builder as an example. In the arch package, users have to assemple an ARCH model starting with an arch.mean model initialized _with_ the data, followed by setting arch.variance and arch.distribution objects, each with their own respective parameters. Keeping this interface would have been highly inflexible and required the user to essentially learn how to use the package from scratch. Inheritting from _Builder allowed the MakeARCH pipe to maintain this flexibility of setting different pieces as well as creating the model's structure before actually having any data (which wouldn't be possible with the original package).


Development Notes on Base Classes
=================================

External <_Node>
----------------

**Configuration:** Sources often require additional ids, secrets or paths in order to access their data. The .config attribute aims to summarise all key configuration details and data needed to access a resource. Additional functions can be added as needed to facilitate one-time connection needs.

**Factories:** Sources, typically web APIs, will give users various functionalities with the same base configurations. The .make() method can be implemeted to return subclasses that inherit parent processing and configuration.

Translator <_Transformer>
-------------------------

Pipe <_Transformer>
-------------------

Model <_Transformer>
--------------------

Applications <Model>
--------------------


Key Concepts, Differences and Philosophy
========================================


<!--### Modularity -->

<!--### Data Integrity-->

<!--### Portability-->

running vs requesting
---------------------

You might have notices that classes that inherit from <Pipe> have .run() methods, classes that inherit from <Node> have .request() methods, both of which return some forrm of data. While these two essentally have the same output functionality, they differ in implementation, where .run() methods get data from a source and modifies is while .request() methods get data, also from some source, and validates it. Thus, the idea of a _DataDef compared to a Pipe becomes clearer.

describing vs tagging
---------------------

The .tags and .desc attributes might seem to be redundant, as both are used to describe some sort of data passing by them and both can be used to search for nodes in the graph. Firsly, and most importantly, the .desc attribute is common to all DataDef instances that inherit from another DataDef, while the .tag attribute is unique to that node, unless it is also present on the parent DataDef or shared with other DataDefs upon instanciation. 

They also do defer in "strictness," as tags will not be checked for truthfulness, while descriptions will be tested on the data, unless, of course, users turn checking off. Tags are included as a feature to allow more flexible, personalizable descriptions thatdescribe groups or structures within the graph rather than a certain functionality.
