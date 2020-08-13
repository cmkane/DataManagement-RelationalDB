Data Management - Relational DB in C++
===============================================
# Project for UCI course CS 222 Data Management

Implementation of a relational database in C++. 
There are several components including:
  - Paged File System
  - Record Based File Manager
  - Relational Manager
  - Index Manager
  - Query Engine

By default you should not change those functions of pre-defined in the given .h files.
If you think some changes are really necessary, please contact us first.

If you are not using CLion and want to use command line make tool:

 - Modify the "CODEROOT" variable in makefile.inc to point to the root
  of your code base if you can't compile the code.
 
- Query Engine (QE)

   Go to folder "qe" and type in:

    make clean
    make
    ./qetest_01

   The program should work. But it does nothing until you implement the extension of RM and QE.

- If you want to try CLI:

   Go to folder "cli" and type in:
   
   make clean
   make
   ./cli_example_01
   
   or
   
   ./start
   

