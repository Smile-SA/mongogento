# Purpose of MongoGento

The module provides an integration of **MongoDB** into **Magento**. The first version handle product attributes and media galleries.

It has been developed and tested against **Magento EE 1.13**.


This module should be deployed on new project with huge catalog (> 100,000 products) since it allows significant reduction of the performance inpact of the EAV model by reducing dramatically the number of attributes stored into the database.


# Install MongoGento

## System requirements

MongoGento requires you to install :

 - MongoDB server >= 2.4 : http://docs.mongodb.org/manual/installation/
 - MongoDB PHP driver on front : you can install it through PECL package http://pecl.php.net/package/mongo

For development environment a single MongoDB instance deployment is sufficient. If you plan a production environment with a more complicated architecture (ReplicaSet or Sharding), you will add to test it strongly on this architecture before it will go live and at least testing environment should reproduce this architecture.


## Module install

The easiest way to install the module is to use the installer, by launching the following shell command from you Magento installation root folder :

    php < < (wget -O - https://raw.github.com/Smile-SA/mongogento/master/installer.php)


The installation will be processed from the master branch. If you prefer to pick a specific release (v.1.0.0 by example), you can use this syntax to specify the release :

    php -- v.1.0.0 < < (wget -O - https://raw.github.com/Smile-SA/mongogento/master/installer.php)

## Configuration

To configure MongoGento, you will have to indicate the configuration of the MongoDB server as shown into the app/etc/mongogento.xml.template file :

    <?xml version="1.0"> 
    <config> 
       <global> 
           <document_db> 
               <connection_string>mongodb://server-name:port/</connection_string> 
               <dbname>my_mongo_database<dbname> 
           </document_db> 
        </global> 
    </config> 



# FAQ

**Is it ready for production ?**

> Previous versions of MongoGento are already into production on some websites. Some with millions of products.

**Is there Magento modules that are reported as non-working with MongoGento ?**

> Yes there is some broken features (the list is not exhaustive) :
>
 - Product catalog rules does not handle some attributes (confirmed
compare does not see MongoDB attributes)
>
The following features have not been tested with MongoGento and should be considered as broken :
>
- Sitemap
- Rule based product relations
- Product tags and comments
- Feel free to submit your report about untested features (OK / KO) and patches for broken features

**What is the Roadmap ?**

> We have several ideas we will evaluate into the roadmap :
>
- Integrate quotes / carts management
- Integrate customer management
- Restore most used broken features
>
Any idea is welcome.
