<?php
/**
 * MongoGento
 *
 * NOTICE OF LICENSE
 *
 * This source file is subject to the Academic Free License (AFL 3.0)
 * that is bundled with this package in the file LICENSE_AFL.txt.
 * It is also available through the world-wide-web at this URL:
 * http://opensource.org/licenses/afl-3.0.php
 *
 * DISCLAIMER
 *
 * Do not edit or add to this file if you wish to upgrade MongoGento to newer
 * versions in the future.
 */

/**
 * Shell script to move existing product data from MySQL to MongoDB
 * This shell depends on the usage of the Smile_Mongogento suite.
 *
 * @category  Smile
 * @package   Smile_Shell
 * @author    Romain Ruaud <romain.ruaud@smile.fr>
 * @copyright 2015 Smile
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 */

require_once dirname(__FILE__) . '/../abstract.php';

/**
 * Shell script to move existing product data from MySQL to MongoDB
 *
 * @category  Smile
 * @package   Smile_Shell
 * @author    Romain Ruaud <romain.ruaud@smile.fr>
 * @copyright 2015 Smile
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 *
 */
class Smile_Shell_Mongoify extends Mage_Shell_Abstract
{
    /** @var int the default store Id */
    const DEFAULT_STORE_VIEW = 0;

    /** @var string The prefix used by Smile_MongoCatalog to store attributes */
    const ATTR_PREFIX = 'attr_';

    /** @var int the chunk size used for reading from Mysql and writing into MongoDB */
    const CHUNK_SIZE = 100;

    /**
     * This collection is used to access to the product collection into MongoDB
     *
     * @var MongoCollection
     */
    protected $_docCollection;

    /** @var array $_sqlAttributeCodes The attributes that will remain stored in MySQL */
    protected $_sqlAttributeCodes = array();

    /** @var null|Smile_MongoCatalog_Model_Resource_Override_Catalog_Product $_catalogResource Mongo dedicated resource model */
    protected $_catalogResource = null;

    /** @var Mage_Catalog_Model_Resource_Product_Attribute_Collection $_productAttributes All current product attributes */
    protected $_productAttributes = null;

    /** @var Mage_Eav_Model_Resource_Entity_Attribute_Set_Collection $_attributesSets All attributes set */
    protected $_attributesSets = null;

    /** @var Varien_Db_Adapter_Pdo_Mysql $_readAdapter MySQL reader */
    protected $_readAdapter = null;

    /** @var Varien_Db_Adapter_Pdo_Mysql $_writeAdapter MySQL writer */
    protected $_writeAdapter = null;

    /** @var int $_updatedDocs Number of imported documents */
    protected $_updatedDocs = 0;

    /** @var int $_ignoredDocs Number of ignored documents */
    protected $_ignoredDocs = 0;

    /** @var int $_updatedGalleries Number of imported galleries */
    protected $_updatedGalleries = 0;

    /** @var int $_ignoredGalleries Number of ignored galleries */
    protected $_ignoredGalleries = 0;

    /** @var array $_statistics an array giving informations on current DB state */
    protected $_statistics = array();

    /**
     * Simple constructor
     *
     * @return Smile_Shell_Mongoify
     */
    public function __construct()
    {
        parent::__construct();

        /** @var Smile_MongoCatalog_Model_Resource_Override_Catalog_Product $catalogResource */
        if (Mage::helper('core')->isModuleEnabled("Smile_MongoCatalog")) {
            $this->_catalogResource = Mage::getResourceModel("catalog/product");
        } else {
            // If module is not enable, instantiate raw object
            // This permits to manage some tests without enabling the module
            $this->_catalogResource = new Smile_MongoCatalog_Model_Resource_Override_Catalog_Product();
        }


        /** @var MongoCollection $_catalogResourceCollection */
        $this->_catalogResourceCollection = $this->_getDocumentCollection();

        $this->_sqlAttributeCodes = $this->_catalogResource->getSqlAttributesCodes();

        $this->_getCurrentStats();
    }

    /**
     * Build report data to have an approximative ID of migration impacts
     *
     * @return array
     */
    protected function _getCurrentStats()
    {
        $productTable = $this->getTable("catalog/product");

        $productCount  = $this->_getReadAdapter()->select()->from($productTable)->reset(Zend_Db_Select::COLUMNS)->columns('COUNT(*)');
        $productNumber = $this->_getReadAdapter()->fetchOne($productCount);

        $this->_statistics['product_number'] = $productNumber;

        foreach ($this->_getAttributesTables() as $tableName) {

            $attributesCount  = $this->_getReadAdapter()
                ->select()
                ->from($tableName)
                ->reset(Zend_Db_Select::COLUMNS)
                ->columns('COUNT(*)');

            $attributesNumber = $this->_getReadAdapter()->fetchOne($attributesCount);

            $this->_statistics[$tableName] = $attributesNumber;

        }

        $galleryTable  = $this->getTable("catalog/product_attribute_media_gallery");
        $galleryCount  = $this->_getReadAdapter()
            ->select()
            ->from($galleryTable)
            ->reset(Zend_Db_Select::COLUMNS)
            ->columns('COUNT(*)');

        $galleryNumber = $this->_getReadAdapter()->fetchOne($galleryCount);
        $this->_statistics["Gallery count"] = $galleryNumber;

        $galleryValuesTable  = $this->getTable("catalog/product_attribute_media_gallery_value");
        $galleryValuesCount  = $this->_getReadAdapter()
            ->select()
            ->from($galleryValuesTable)
            ->reset(Zend_Db_Select::COLUMNS)
            ->columns('COUNT(*)');

        $galleryValuesNumber = $this->_getReadAdapter()->fetchOne($galleryValuesCount);
        $this->_statistics["Gallery values count"] = $galleryValuesNumber;

    }

    /**
     * Print current DB statistics
     *
     * @throws Zend_Text_Table_Exception
     *
     * @return void Nothing
     */
    protected function _printStatistics()
    {
        echo "============================================================================================================\n";
        echo "Current statistics :\n";
        $table = new Zend_Text_Table(array('columnWidths' => array(60, 40)));
        foreach ($this->_statistics as $statisticKey => $value) {
            $table->appendRow(array($statisticKey, $value));
        }

        echo (string) $table;
        echo "\n";
    }

    /**
     * Retrieve elapsed time since migration start
     *
     * @return float elapsed time (s)
     */
    protected function _getElapsedTime()
    {
        $this->_endTime   = microtime(true);
        $this->_totalTime = $this->_endTime - $this->_startTime;

        return $this->_totalTime;
    }

    /**
     * Print elapsed time since migration start
     *
     * @param bool $container Display container or not
     *
     * @return string elapsed time
     */
    protected function _printElapsedTime($container = true)
    {
        $elapsedTime = $this->_getElapsedTime();
        if ($container) {
            return "[" . gmdate("H:i:s", $elapsedTime) . "]";
        } else {
            return gmdate("H:i:s", $elapsedTime);
        }
    }

    /**
     * Use the mongo adapter to get access to a collection used as storage for products.
     * The collection used has the same name as main entity table (catalog_product_entity).
     *
     * @return MongoCollection The Mongo document collection
     */
    protected function _getDocumentCollection()
    {
        if (is_null($this->_docCollection)) {
            $adapter = Mage::getSingleton('mongocore/resource_connection_adapter');
            $collectionName = Mage::getResourceModel('catalog/product')->getEntityTable();
            $this->_docCollection = $adapter->getCollection($collectionName);
        }

        return $this->_docCollection;
    }

    /**
     * Implementation of the run method that will launch the migration if asked for
     *
     * @see Mage_Shell_Abstract::run()
     *
     * @return void
     */
    public function run()
    {
        if (isset($this->_args['dump-tables']) || isset($this->_args['process']) || isset($this->_args['clean-tables'])) {
            if (isset($this->_args['dump-tables']) && $this->_args['dump-tables'] === true) {
                $this->_dumpAttributesTables();
            }

            if (isset($this->_args['process']) && $this->_args['process'] === true) {
                $this->runMigration();
            }

            if (isset($this->_args['clean-tables']) && $this->_args['clean-tables'] === true) {
                $this->_cleanAttributesTables();
                $this->_cleanGalleriesTables();
            }
        } else {
            echo $this->usageHelp();
        }
    }

    /**
     * Will run the migration process that is divided in several steps :
     *
     *     - prepare SQL attributes list, that will remain on MySQL tables
     *     - loop on current SQL attributes values for products, then push it to MongoDB
     *     - loop on current SQL galleries for products, then push it to MongoDB
     *     - process post migration tasks : reindexing, cache clearing.
     *
     * @return void Nothing
     */
    public function runMigration()
    {
        $this->_beforeMigration();

        $this->_migrateAttributesValues();

        $this->_migrateGalleriesValues();

        $this->_afterMigration();
    }

    /**
     * Process a raw mysqldump of eav attributes and gallery tables
     *
     * @return void Nothing
     */
    protected function _dumpAttributesTables()
    {
        $dbConfig = $this->_getReadAdapter()->getConfig();

        $host = !empty($dbConfig['unix_socket']) ? $dbConfig['unix_socket']
            : (!empty($dbConfig['host']) ? $dbConfig['host'] : 'localhost');

        $dbname   = $dbConfig['dbname'];
        $user     = $dbConfig['username'];
        $password = $dbConfig['password'];

        $path = Mage::getBaseDir("export");

        $filename    = "attributes_tables_" . date("d-m-Y");
        $destination = $path . DS . $filename;

        $tables = implode(" ", $this->_getTables());

        echo "Dumping attributes values and gallery tables to {$destination} ... \n";
        echo "This can take several minutes... please wait... \n\n";

        $mysqldump = "mysqldump --user={$user} --password='{$password}' --host={$host} {$dbname} {$tables} --routines --triggers "
            . " > {$destination}.sql";

        exec($mysqldump);
    }

    /**
     * Retrieve tables that are impacted by the migration :
     *
     *     - EAV tables (catalog_product_entity_*)
     *     - Gallery tables
     *
     * @return array
     */
    protected function _getTables()
    {
        return array_merge(
            (array) $this->_getAttributesTables(),
            (array) $this->_getGalleryTables()
        );
    }

    /**
     * Retrieve gallery related tables
     *
     * @return array
     */
    protected function _getGalleryTables()
    {
        return array(
            $this->getTable("catalog/product_attribute_media_gallery"),
            $this->getTable("catalog/product_attribute_media_gallery_value")
        );
    }

    /**
     * Retrieve attributes related tables
     *
     * @return array
     */
    protected function _getAttributesTables()
    {
        $tableNames = array();

        $attributes = array(
            'int'       => array_keys($this->_getAttributes('int')),
            'varchar'   => array_keys($this->_getAttributes('varchar')),
            'text'      => array_keys($this->_getAttributes('text')),
            'decimal'   => array_keys($this->_getAttributes('decimal')),
            'datetime'  => array_keys($this->_getAttributes('datetime')),
        );

        foreach (array_keys($attributes) as $backendType) {
            $tableNames[] = $this->getTable(array('catalog/product', $backendType));
        }

        return $tableNames;
    }

    /**
     * Prepare the migration
     *
     * @return Smile_Shell_Mongoify
     */
    protected function _beforeMigration()
    {
        $this->_startTime = microtime(true);
        return $this;
    }

    /**
     * Retrieve all attributes set
     *
     * @return array
     */
    protected function _getAllAttributesSet()
    {
        if (is_null($this->_attributesSets)) {
            $this->_attributesSets = array();
            $attributeSets = Mage::getResourceModel('eav/entity_attribute_set_collection');
            foreach ($attributeSets as $attributeSetId => $attributeSet) {
                $this->_attributesSets[$attributeSetId] = $attributeSet;
            }
        }

        return $this->_attributesSets;
    }

    /**
     * Retrieve an attribute set
     *
     * @param int $attributeSetId The attribute set id
     *
     * @throws Mage_Core_Exception
     *
     * @return Mage_Eav_Model_Entity_Attribute_Set
     */
    protected function _getAttributesSet($attributeSetId = null)
    {
        $attributesSet = $this->_getAllAttributesSet();
        if (isset($attributesSet[$attributeSetId])) {
            return $attributesSet[$attributeSetId];
        } else {
            Mage::throwException("Unable to find attribute set for id {$attributeSetId}");
        }
    }

    /**
     * Migrate attributes values from MySQL to MongoDB for a dedicated store
     *
     * @param array $productIds The product ids to migrate
     *
     * @return Smile_Shell_Mongoify
     */
    protected function _migrateAttributesValues($productIds = null)
    {
        $staticFields = array();
        foreach ($this->_getAttributes('static') as $attribute) {
            $staticFields[] = $attribute->getAttributeCode();
        }

        $dynamicFields = array(
            'int'       => array_keys($this->_getAttributes('int')),
            'varchar'   => array_keys($this->_getAttributes('varchar')),
            'text'      => array_keys($this->_getAttributes('text')),
            'decimal'   => array_keys($this->_getAttributes('decimal')),
            'datetime'  => array_keys($this->_getAttributes('datetime')),
        );

        $lastProductId = 0;

        while (true) {

            $productDocuments = array(); // Will contain mongoDB documents to insert

            // Retrieve "fixed" data for products : data from catalog_product_entity
            $products = $this->_getProducts($staticFields, $productIds, $lastProductId);
            if (!$products) {
                echo "All products processed. Breaking\n\n";
                break;
            }

            foreach ($products as $productData) {
                $lastProductId = $productData['entity_id'];
                // Non-scoped attributes stored into catalog_product_entity
                $productFixAttributes[$productData['entity_id']] = $productData;
                $productIds[] = (int) $productData['entity_id']; // Explicit cast to integer, to have better MySQL filter
            }

            $storeIds = array_keys(Mage::app()->getStores(true));
            asort($storeIds); // Ensure store 0 will come in first

            // Retrieve attributes values for stores
            foreach ($storeIds as $storeId) {

                $productAttributes = $this->_getProductAttributes($storeId, $productIds, $dynamicFields);

                foreach ($products as $productData) {
                    if (!isset($productFixAttributes[$productData['entity_id']])) {
                        continue;
                    }

                    if (!isset($productAttributes[$productData['entity_id']])) {
                        continue;
                    }

                    // Combine fixed data from catalog_product_entity and EAV attributes values for default store
                    if ($storeId == self::DEFAULT_STORE_VIEW) {
                        $productDocuments[$productData['entity_id']][self::ATTR_PREFIX . $storeId] = array_merge(
                            $productFixAttributes[$productData['entity_id']],
                            $productAttributes[$productData['entity_id']]
                        );
                    } else {
                        // For non-default store view, only get EAV values
                        $productDocuments[$productData['entity_id']][self::ATTR_PREFIX . $storeId]
                            = $productAttributes[$productData['entity_id']];
                    }
                }
            }

            $this->_saveProductDocuments($productDocuments);

            $productIds = array();   // Will contain product ids next turn
        }

        return $this;
    }

    /**
     * Save Multiple products into MongoDB
     *
     * @param array $productDocuments The product data to store into MongoDB
     *
     * @return Smile_Shell_Mongoify
     */
    protected function _saveProductDocuments($productDocuments)
    {
        foreach ($productDocuments as &$productDocument) {

            $mongoDocument = $this->_prepareDocumentForSave($productDocument);

            $mongoId       = (int) $mongoDocument[self::ATTR_PREFIX . self::DEFAULT_STORE_VIEW]["entity_id"];

            try {

                $this->_catalogResource->updateRawDocument($mongoId, $mongoDocument);
                $this->_updatedDocs++;
                echo ".";

            } catch (Exception $exception) {
                Mage::logException($exception);
                Mage::log("Product {$mongoId} Ignored : ", null, "mongoify.log");
                Mage::log($mongoDocument);
                $this->_ignoredDocs++;
            }
        }

        echo " {$this->_updatedDocs} / {$this->_statistics['product_number']} Products" .
            " ({$this->_ignoredDocs} ignored)"
            . " | Elapsed time : {$this->_printElapsedTime()}\n";

        return $this;
    }

    /**
     * Prepare a product before saving it into MongoDB
     *
     * @param array $productDocument The product data to store into MongoDB
     *
     * @return array|null
     */
    protected function _prepareDocumentForSave($productDocument)
    {
        $productEntityFields = $this->_getWriteAdapter()->describeTable($this->getTable("catalog/product"));

        $mongoDocument = null;

        if ($productDocument && (is_array($productDocument)) && (!empty($productDocument))) {

            $productAttributeSet = $this->_getAttributesSet(
                $productDocument[self::ATTR_PREFIX . self::DEFAULT_STORE_VIEW]["attribute_set_id"]
            );

            foreach ($productDocument as &$storeValues) {

                foreach ($storeValues as $attributeCode => &$value) {

                    // Prepare values that are not coming from static fields
                    if (!in_array($attributeCode, array_keys($productEntityFields))) {
                        $attribute = $this->_getAttributeByCode($attributeCode);
                        if ($attribute->isInSet($productAttributeSet->getId())) {
                            $value = $this->_prepareValueForDocumentSave($attribute, $value);
                        } else {
                            // Unset products attributes that should not be in the product attribute set
                            // This should not happen, according to the EAV model, but better ensure it.
                            unset($storeValues[$attributeCode]);
                        }
                    }
                }
            }

            $mongoDocument = $productDocument;
        }

        return $mongoDocument;
    }

    /**
     * Ensure values are OK for numeric attributes before saving them
     *
     * @param Mage_Eav_Model_Entity_Attribute_Abstract $attribute Attribute to prepare the value for
     * @param mixed                                    $value     Value to be saved
     *
     * @return mixed The value ready for save
     */
    protected function _prepareValueForDocumentSave($attribute, $value)
    {
        if (is_object($value)) {
            $value = (string) $value;
        }

        if ($value != '' && (!is_null($attribute))) {
            $type  = strtolower($attribute->getBackendType());

            // Handle numeric cast to float
            if ($type == 'decimal' || $type == 'numeric' || $type == 'float') {
                $value = Mage::app()->getLocale()->getNumber($value);
            }

            if ($type == "int") {
                // Ensure int storage is correct
                $value = (int) $value;
            }
        }

        return $value;
    }

    /**
     * Load product(s) attributes values for a given store
     *
     * @param int   $storeId        The store Id
     * @param array $productIds     The product Ids
     * @param array $attributeTypes The attributes backend types
     *
     * @return array
     */
    protected function _getProductAttributes($storeId, array $productIds, array $attributeTypes)
    {
        $result  = array();
        $selects = array();
        $adapter = $this->_getWriteAdapter();
        $ifStoreValue = $adapter->getCheckSql('t_store.value_id > 0', 't_store.value', 't_default.value');
        foreach ($attributeTypes as $backendType => $attributeIds) {
            if ($attributeIds) {
                $tableName = $this->getTable(array('catalog/product', $backendType));
                $selects[] = $adapter->select()
                    ->from(
                        array('t_default' => $tableName),
                        array('entity_id', 'attribute_id', 'store_id')
                    )
                    ->joinLeft(
                        array('t_store' => $tableName),
                        $adapter->quoteInto(
                            't_default.entity_id=t_store.entity_id' .
                            ' AND t_default.attribute_id=t_store.attribute_id' .
                            ' AND t_store.store_id=?',
                            $storeId
                        ),
                        array('value' => $this->_unifyField($ifStoreValue, $backendType))
                    )
                    ->where('t_default.store_id=?', $storeId)
                    ->where('t_default.attribute_id IN (?)', $attributeIds)
                    ->where('t_default.entity_id IN (?)', $productIds);
            }
        }

        if ($selects) {
            $select = $adapter->select()->union($selects, Zend_Db_Select::SQL_UNION_ALL);
            $query = $adapter->query($select);

            while ($row = $query->fetch()) {
                $result[$row['entity_id']][$this->_getAttributeCodeById($row['attribute_id'])] = $row['value'];
            }
        }

        return $result;
    }

    /**
     * Retrieve products data from the catalog_product_entity table per store
     *
     * @param array     $staticFields  The static fields to retrieve
     * @param array|int $productIds    The product ids
     * @param int       $lastProductId The last product Id
     *
     * @return array
     */
    protected function _getProducts(array $staticFields, $productIds = null, $lastProductId = 0)
    {
        $readAdapter   = $this->_getReadAdapter();

        $select = $readAdapter->select()
            ->useStraightJoin(true)
            ->from(
                array('e' => $this->getTable('catalog/product')),
                array_merge(array('entity_id', 'type_id', 'attribute_set_id', 'entity_type_id'), $staticFields)
            );

        if (!is_null($productIds) && !empty($productIds)) {
            $select->where('e.entity_id IN(?)', $productIds);
        }

        $select->where('e.entity_id>?', $lastProductId)
            ->limit(self::CHUNK_SIZE)
            ->order('e.entity_id');

        $result = $readAdapter->fetchAll($select);

        return $result;
    }

    /**
     * Retrieve attributes for a given backend type
     *
     * @param string $backendType The backend type of attributes : int, varchar, text, decimal
     *
     * @return Mage_Catalog_Model_Resource_Product_Attribute_Collection
     */
    protected function _getAttributes($backendType = null)
    {
        if (is_null($this->_productAttributes)) {
            $this->_productAttributes = array();

            /** @var Mage_Catalog_Model_Resource_Product_Attribute_Collection $productAttributes */
            $productAttributes = Mage::getResourceModel('catalog/product_attribute_collection');
            //$productAttributes->addFieldToFilter('attribute_code', array('nin' => $this->_sqlAttributeCodes));

            $attributes = $productAttributes->getItems();

            $entity = $this->getEavConfig()
                ->getEntityType(Mage_Catalog_Model_Product::ENTITY)
                ->getEntity();

            foreach ($attributes as $attribute) {
                $attribute->setEntity($entity);
            }

            $this->_productAttributes = $attributes;
        }

        if (!is_null($backendType)) {
            $attributes = array();
            foreach ($this->_productAttributes as $attributeId => $attribute) {
                // Do not retrieve this static attribute because it is not a column of catalog_product_entity
                if (($backendType == "static" ) && ($attribute->getAttributeCode() == "category_ids")) {
                    continue;
                }
                if ($attribute->getBackendType() == $backendType) {
                    $attributes[(int) $attributeId] = $attribute;
                }
            }

            return $attributes;
        }

        return $this->_productAttributes;
    }

    /**
     * Retrieve Attribute Code by Attribute Id
     *
     * @param string $attributeId The attribute Id
     *
     * @throws Mage_Core_Exception
     * @return string
     */
    protected function _getAttributeCodeById($attributeId)
    {
        $attributes    = $this->_getAttributes();
        $attributeCode = null;

        foreach ($attributes as $_attribute) {
            if ($_attribute->getAttributeId() == $attributeId) {
                $attributeCode = $_attribute->getAttributeCode();
            }
        }

        if (is_null($attributeCode)) {
            Mage::throwException("Unable to find attribute code for Id {$attributeId}");
        }

        return $attributeCode;
    }

    /**
     * Retrieve Attribute by its code
     *
     * @param string $attributeCode The attribute Code
     *
     * @throws Mage_Core_Exception
     * @return Mage_Eav_Model_Attribute
     */
    protected function _getAttributeByCode($attributeCode)
    {
        $attributes    = $this->_getAttributes();
        $attribute     = null;

        foreach ($attributes as $_attribute) {
            if ($_attribute->getAttributeCode() == $attributeCode) {
                $attribute = $_attribute;
            }
        }

        return $attribute;
    }

    /**
     * Returns expresion for field unification
     *
     * @param string $field       The field to clean
     * @param string $backendType The backend type
     *
     * @return Zend_Db_Expr
     */
    protected function _unifyField($field, $backendType = 'varchar')
    {
        if ($backendType == 'datetime') {
            $expr = Mage::getResourceHelper('catalog')->castField(
                $this->_getReadAdapter()->getDateFormatSql($field, '%Y-%m-%d %H:%i:%s')
            );
        } else {
            $expr = Mage::getResourceHelper('catalog')->castField($field);
        }
        return $expr;
    }

    /**
     * Retrieve EAV Config Singleton
     *
     * @return Mage_Eav_Model_Config
     */
    public function getEavConfig()
    {
        return Mage::getSingleton('eav/config');
    }

    /**
     * Retreive table name
     *
     * @param string $alias The table alias
     *
     * @return string
     */
    public function getTable($alias)
    {
        return Mage::getSingleton('core/resource')->getTableName($alias);
    }

    /**
     * Migrate galleries values from MySQL to MongoDB
     *
     * @return Smile_Shell_Mongoify
     */
    protected function _migrateGalleriesValues()
    {
        /**
         * Initial gallery structure :
         *
         * mysql> select * from catalog_product_entity_media_gallery where entity_id=1505;
        +----------+--------------+-----------+----------------------------+
        | value_id | attribute_id | entity_id | value                      |
        +----------+--------------+-----------+----------------------------+
        |     1429 |           77 |      1505 | /p/i/picture_star_1647.jpg |
        +----------+--------------+-----------+----------------------------+


        mysql> select * from catalog_product_entity_media_gallery_value where value_id=1429;
        +----------+----------+-------+----------+----------+
        | value_id | store_id | label | position | disabled |
        +----------+----------+-------+----------+----------+
        |     1429 |        0 | NULL  |        1 |        0 |
        |     1429 |        1 |       |        1 |        0 |
        |     1429 |        2 |       |        1 |        0 |
        +----------+----------+-------+----------+----------+

         */

        /**
         * Expected structure :
         *
        "galleries" : {
            "media_gallery" : [
                {
                    "file" : "/p/i/picture_star_1647.jpg",
                    "value_id" : "2d5c1c756ad0d829d28ac629c02159a1", <= this is the md5(value_id)
                    "attr_0" : {
                        "label" : "",
                        "position" : 1,
                        "disabled" : "0"
                    }
                }
            ]
        }

         */

        $galleryTable = $this->getTable("catalog/product_attribute_media_gallery");

        $gallerySelect = $this->_getReadAdapter()->select();
        $gallerySelect->from($galleryTable);

        /** @var Mage_Core_Model_Resource_Iterator $iterator */
        $iterator = Mage::getResourceModel("core/iterator");

        $iterator->walk(
            $gallerySelect,
            array(array($this, 'galleryCallback'))
        );

        echo "All galleries processed ... \n";
        return $this;
    }

    /**
     * Callback called by iterator when looping on galleries
     *
     * @param arary $args The callback arguments
     *
     * @return void Nothing
     */
    public function galleryCallback($args)
    {
        $row             = $args['row'];      // A row from catalog_product_attribute_media_gallery
        $entityId        = $row['entity_id']; // Product Id

        // It seems that attribte_id can vary, actually only "media_gallery" exists on Magento,
        // but we will keep consistency with this variant possibility
        $attributeCode   = $this->_getAttributeCodeById($row['attribute_id']);

        // We do not want to insert these data into Mongo
        unset($row["entity_id"]);
        unset($row["attribute_id"]);

        // Retrieve all media values for different stores related to current media file
        $galleryValueTable  = $this->getTable("catalog/product_attribute_media_gallery_value");
        $galleryValueSelect = $this->_getReadAdapter()->select();
        $galleryValueSelect
            ->from($galleryValueTable)
            ->where(new Zend_Db_Expr("value_id = {$row['value_id']}"));

        $values = $this->_getReadAdapter()->fetchAll($galleryValueSelect);

        // Inject stores values into file related data
        if (is_array($values) && (!empty($values))) {
            foreach ($values as $galleryValue) {
                $galleryValue['position'] = (int) $galleryValue['position'];
                $row['attr_' . $galleryValue['store_id']] = $galleryValue;
                // We do not want to insert these data into Mongo
                unset($row['attr_' . $galleryValue['store_id']]['value_id']);
                unset($row['attr_' . $galleryValue['store_id']]['store_id']);
            }
        }

        // value_id is md5ified on the Smile_MongoCatalog module
        $row['value_id'] = md5($row['value_id']);

        // store filename into file key
        $row['file'] = $row['value'];
        unset($row['value']);

        // Prepare update
        $updateFilter = array('_id' => new MongoInt32($entityId));
        $updateValue = array(
            '$addToSet' => array( // $addToSet because galleries.media_gallery is an array
                'galleries.' . $attributeCode => array(
                    '$each' => array(// $each is used because of the $addToSet
                        (array) $row
                    )
                )
            )
        );

        // Update the document
        $update = $this->_catalogResourceCollection->update($updateFilter, $updateValue, array('upsert' => true));
        echo ".";
        if ($update) {
            $this->_updatedGalleries++;
        } else {
            $this->_ignoredGalleries++;
        }
        if (($this->_updatedGalleries % self::CHUNK_SIZE ) == 0) {
            echo "X {$this->_updatedGalleries} / {$this->_statistics['Gallery count']} Galleries"
                . "({$this->_ignoredGalleries} ignored)"
                . "Elapsed time : {$this->_printElapsedTime()}\n";
        }
    }

    /**
     * Process product reindexation and clear cache
     *
     * @return Smile_Shell_Mongoify
     */
    protected function _afterMigration()
    {
        echo "\n\n Migration total time : {$this->_printElapsedTime(false)} \n\n";

        if ($this->getArg("reindex-after")) {
            $this->_reindexAll();
        }

        $this->_cleanCache();

        return $this;
    }

    /**
     * Clear cache after migration
     *
     * @return void Nothing
     */
    protected function _cleanCache()
    {
        $cacheInstance = Mage::app()->getCacheInstance();

        // Using API to clean all caches
        $configTags = array();
        foreach ($cacheInstance->getTypes() as $type) {
            $tags = explode(',', $type->getTags());
            $configTags = array_merge($configTags, $tags);
        }
        Mage::app()->cleanCache($configTags);
        Mage::dispatchEvent('adminhtml_cache_flush_system');
        echo implode(", ", $configTags) . " cache tags purged\n";

        // Using API to clean catalog images cache
        try {
            Mage::getModel('catalog/product_image')->clearCache();
            Mage::dispatchEvent('clean_catalog_images_cache_after');
            echo "The image cache was cleaned.\n";
        } catch(Exception $e) {
            echo $e->getMessage() . "\n";
        }

        if (Mage::helper("core")->isModuleEnabled("Smile_MageCache")) {
            Mage::getSingleton('smile_magecache/processor')->cleanCache();
        }
    }

    /**
     * Rebuild all Magento's indexes
     *
     * @return void Nothing
     */
    protected function _reindexAll()
    {
        /** @var Mage_Index_Model_Indexer $indexer */
        $indexer   = Mage::getSingleton('index/indexer');
        $processes = $indexer->getProcessesCollection();

        echo "Processing full reindex\n";
        foreach ($processes as $process) {
            /* @var $process Mage_Index_Model_Process */
            try {
                $process->reindexEverything();
                echo $process->getIndexer()->getName() . " index was rebuilt successfully\n";
            } catch (Mage_Core_Exception $e) {
                echo $e->getMessage() . "\n";
            } catch (Exception $e) {
                echo $process->getIndexer()->getName() . " index process unknown error:\n";
                echo $e . "\n";
            }
        }
    }

    /**
     * Cleans attribute values tables that have been moved into MongoDB
     *
     * @return $this
     */
    protected function _cleanAttributesTables()
    {
        echo "Cleaning old attributes tables ...\n";

        $writeAdapter = $this->_getWriteAdapter();

        $attributeIds = $this->_getReadAdapter()
            ->select()
            ->from($this->getTable("eav/attribute"), "attribute_id")
            ->where("attribute_code IN (?)", $this->_sqlAttributeCodes);

        $sqlAttributesIds = $this->_getReadAdapter()->fetchCol($attributeIds);

        // @see Smile_MongoCatalog_Model_Resource_Override_Catalog_Product::getSqlAttributesCodes()
        $expectedNumber = (
            count($this->_catalogResource->getSqlAttributesCodes())
            - count($this->_getWriteAdapter()->describeTable($this->getTable("catalog/product")))
        );

        // I consider that if we got less than the "normal" number of attributes, something could have gone wrong
        // Throwing this exception can save a DELETE FROM with an empty clause
        if (empty($sqlAttributesIds) || (count($sqlAttributesIds) < $expectedNumber)) {
            Mage::throwException(
                "It seems that there is less attributes to keep in MySQL (" . count($sqlAttributesIds) . ")"
                . " than expected in the native Smile_MongoCatalog module ({$expectedNumber}) ..."
                . " This could indicate an error, feel free to check and comment theses lines if needed."
            );
        }

        $attributeIds = implode(",", $sqlAttributesIds);

        foreach ($this->_getAttributesTables() as $table) {
            echo "Deleting values from {$table} \n";
            $writeAdapter->delete($table, new Zend_Db_Expr("attribute_id NOT IN ({$attributeIds})"));
        }

        return $this;
    }

    /**
     * Cleans galleries values tables that have been moved into MongoDB
     *
     * @return $this
     */
    protected function _cleanGalleriesTables()
    {
        echo "Cleaning old galleries tables ...\n";

        foreach ($this->_getGalleryTables() as $table) {
            echo "Deleting values from {$table} \n";
            $this->_getWriteAdapter()->delete($table);
        }

        return $this;
    }

    /**
     * Get read Adapter, only once
     *
     * @return Varien_Db_Adapter_Pdo_Mysql
     */
    protected function _getReadAdapter()
    {
        if (is_null($this->_readAdapter)) {
            $this->_readAdapter = Mage::getModel('core/resource')->getConnection('read');
        }
        return $this->_readAdapter;
    }

    /**
     * Get writer Adapter, only once
     *
     * @return Varien_Db_Adapter_Pdo_Mysql
     */
    protected function _getWriteAdapter()
    {
        if (is_null($this->_writeAdapter)) {
            $this->_writeAdapter = Mage::getModel('core/resource')->getConnection('write');
        }
        return $this->_writeAdapter;
    }

    /**
     * Implementation of help method
     *
     * @see Mage_Shell_Abstract::usageHelp()
     *
     * @return string The help message
     */
    public function usageHelp()
    {
        $usage = <<<USAGE

Usage:  php mongoify.php [option]

This scripts migrates data from EAV attributes tables that are related to "product" entities.
Basically, it will copy data contained into these tables from MySQL to MongoDB.
You can perform a backup of theses tables before, and clean it after they have been migrated.

This script process insert in MongoDB with an update() syntax with upsert.
So it can be launched several times if you do not choose to clean tables after.

You can run the script without enabling Smile_MongoCatalog (in case you need to preserve MySQL behavior during migration)
You can NOT run the script without enabling Smile_MongoCore (this module is needed to get the MongoDB Adapter)

--help                             This help

--dump-tables                      Backup tables : if combined with --process, will backup tables before migration
--process                          Launch the migration process
--clean-tables                     Clean tables : if combined with --process, will clean tables after migration
--reindex-after                    Reindex after : if combined with --process, rebuild all Magento's indexes after migration

/!\ BEWARE : Doing a --dump-tables is mandatory to backup data if needed /!\

USAGE;

        return $usage . $this->_printStatistics();

    }
}

// Run the script
$shell = new Smile_Shell_Mongoify();
$shell->run();