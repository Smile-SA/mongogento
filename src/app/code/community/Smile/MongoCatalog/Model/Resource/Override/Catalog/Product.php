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
 * Product entity resource model using MongoDB for attributes storage
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCatalog_Model_Resource_Override_Catalog_Product extends Mage_Catalog_Model_Resource_Product
{

    /**
     * Following attributes are stored into MongoDB but are also kept into MySQL into their respective backend tables.
     * This is done to kept indexes working and some other features like reports
     *
     * @var array
     */
    protected $_sqlAttributeCodes = null;


    /**
     * Ids of the attributes which are also stored into MySQL
     *
     * @var array
     */
    protected $_sqlAttributeIds = null;


    /**
     * This collection is used to access to the product collection into MongoDB
     *
     * @var MongoCollection
     */
    protected $_docCollection;


    /**
     * This methods returns the list of the attribute that have been kept into
     * MySQL for better compatibility with Magento features.
     *
     * @return array Array of the attribute stored into MySQL
     */
    public function getSqlAttributesCodes()
    {
        if (is_null($this->_sqlAttributeCodes)) {

            $this->_sqlAttributeCodes = array();

            $specialAttributes = array(
                'visibility', 'status', 'price', 'tax_class_id', 'name', 'url_key', 'url_path',
                'special_price', 'special_from_date', 'special_to_date', 'msrp', 'price_type'
            );

            $staticFields = $this->_getWriteAdapter()->describeTable($this->getEntityTable());

            $this->_sqlAttributeCodes = array_merge($specialAttributes, array_keys($staticFields));


        }


        return $this->_sqlAttributeCodes;
    }

    /**
     * In some case we would better use attribute ids than attribute codes for attribute stored into MySQL
     * This method returns these ids.
     *
     * @return array The ids of the attributes stored into MySQL
     */
    protected function _getSqlAttributesIds()
    {
        if ($this->_sqlAttributeIds === null) {

            $this->_sqlAttributeIds = array();

            foreach ($this->getSqlAttributesCodes() as $attributeCode) {

                if (isset($this->_attributesByCode[$attributeCode])) {
                    $this->_sqlAttributeIds[] = $this->_attributesByCode[$attributeCode]->getAttributeId();
                }
            }
        }

        return $this->_sqlAttributeIds;
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
            $adapter = $this->getAdapter();
            $this->_docCollection = $adapter->getCollection($this->getEntityTable());
        }

        return $this->_docCollection;
    }

    /**
     * Retrieve connection to MongoDB
     *
     * @return Smile_MongoCore_Model_Resource_Connection_Adapter
     */
    public function getAdapter()
    {
        return Mage::getSingleton('mongocore/resource_connection_adapter');
    }


    /**
     * Allow to build a product id filter for the collection more easily.
     *
     * @param array|int $entityIds The id of the product the filter must target
     *
     * @return array The Mongo filter on product ids
     */
    public function getIdsFilter($entityIds)
    {
        return $this->getAdapter()->getQueryBuilder()->getIdsFilter($entityIds);
    }


    /**
     * Save object collected data : method overridded since :
     *
     *  - only few attributes should be saved into the MySQL database (l.185 and 198)
     *
     *  - append document collection storage for al attributes (l.226)
     *
     * @param array $saveData array('newObject', 'entityRow', 'insert', 'update', 'delete')
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product Self reference
     */
    protected function _processSaveData($saveData)
    {
        extract($saveData);
        /**
         * Import variables into the current symbol table from save data array
         *
         * @see Mage_Eav_Model_Entity_Attribute_Abstract::_collectSaveData()
         *
         * @var array $entityRow
         * @var Mage_Core_Model_Abstract $newObject
         * @var array $insert
         * @var array $update
         * @var array $delete
         */
        $adapter        = $this->_getWriteAdapter();
        $insertEntity   = true;
        $entityTable    = $this->getEntityTable();
        $entityIdField  = $this->getEntityIdField();
        $entityId       = $newObject->getId();

        unset($entityRow[$entityIdField]);
        if (!empty($entityId) && is_numeric($entityId)) {
            $bind   = array('entity_id' => $entityId);
            $select = $adapter->select()
                ->from($entityTable, $entityIdField)
                ->where("{$entityIdField} = :entity_id");
            $result = $adapter->fetchOne($select, $bind);
            if ($result) {
                $insertEntity = false;
            }
        } else {
            $entityId = null;
        }

        /**
         * Process base row
         */
        $entityObject = new Varien_Object($entityRow);
        $entityRow    = $this->_prepareDataForTable($entityObject, $entityTable);
        if ($insertEntity) {
            if (!empty($entityId)) {
                $entityRow[$entityIdField] = $entityId;
                $adapter->insertForce($entityTable, $entityRow);
            } else {
                $adapter->insert($entityTable, $entityRow);
                $entityId = $adapter->lastInsertId($entityTable);
            }
            $newObject->setId($entityId);
        } else {
            $where = sprintf('%s=%d', $adapter->quoteIdentifier($entityIdField), $entityId);
            $adapter->update($entityTable, $entityRow, $where);
        }

        $sqlAttributesIds = $this->_getSqlAttributesIds();

        /**
         * insert attribute values
         */
        if (!empty($insert)) {
            foreach ($insert as $attributeId => $value) {
                if (in_array($attributeId, $sqlAttributesIds)) {
                    $attribute = $this->getAttribute($attributeId);
                    $this->_insertAttribute($newObject, $attribute, $value);
                }
            }
        }

        /**
         * update attribute values
         */
        if (!empty($update)) {
            foreach ($update as $attributeId => $v) {
                if (in_array($attributeId, $sqlAttributesIds)) {
                    $attribute = $this->getAttribute($attributeId);
                    $this->_updateAttribute($newObject, $attribute, $v['value_id'], $v['value']);
                }
            }
        }

        /**
         * delete empty attribute values
         */
        if (!empty($delete)) {
            foreach ($delete as $table => $values) {
                $this->_deleteAttributes($newObject, $table, $values);
            }
        }

        foreach ($this->_attributeValuesToSave as $table => $attributes) {
            foreach ($attributes as $index => $attribute) {
                if (!in_array($attribute['attribute_id'], $sqlAttributesIds)) {
                    unset($this->_attributeValuesToSave[$table][$index]);
                }
            }
        }

        $this->_processAttributeValues();

        // Retrieve the collection to be updated
        // Provide a raw MongoCollection object pointing
        // to catalog_product_entity collection
        $collection = $this->_getDocumentCollection();

        // We update only the document matching the currently edited product
        $updateCond = $this->getIdsFilter($newObject->getId());

        $updateData = $this->_getSaveAllAttributesData($newObject, $saveData, $insertEntity);

        $collection->update($updateCond, array('$set' => $updateData), array('upsert' => true));

        $newObject->isObjectNew(false);

        return $this;
    }


    /**
     * Save product attributes into the document collection.
     *
     * Attribute have to stored by scope. Example :
     *    - attributes of the global scope are stored into the attr_0 field of the document
     *    - attributes of the store 1 are stored into the attr_1 field of the document
     *
     * Values have to been set for the global scope when saving a new product
     *
     * @param Mage_Catalog_Model_Product $object       The product to be saved
     * @param array                      $data         The new data of the product to be saved
     * @param bool                       $isProductNew Indicates if the product is a new one or not
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product Self reference
     */
    protected function _getSaveAllAttributesData($object, $data, $isProductNew)
    {
        // Place at least id as saved field into the document (only mandatory field)
        $updateData = array();

        foreach ($this->_attributesByCode as $attribute) {

            // If the attribute is empty we do not sore anyting into the DB
            // Attribute storage format have already been process in previous steps
            $value = $object->getData($attribute->getAttributeCode());

            if ($value !== null && $value !== false) {

                // By default => attribute data sould be stored into the product current store scope
                $storeId = 'attr_' . $object->getStoreId();

                if ($isProductNew === true || $attribute->isScopeGlobal()) {
                    // If product is new we store it into the default store instead of the current one
                    $storeId = 'attr_' . $this->getDefaultStoreId();
                }

                if (!is_string($value) || $value != '') {
                    // Push saved values into the saved document
                    $fieldName = $storeId . '.' . $attribute->getAttributeCode();
                    $updateData[$fieldName] = $this->_prepareValueForDocumentSave($attribute, $value);
                }

            }
        }

        return $updateData;
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

        if ($value != '') {
            $type  = strtolower($attribute->getBackendType());

            // Handle numeric cast to float
            $value = $this->_prepareTableValueForSave($value, $attribute->getBackendType());

            if ($type == "int") {
                // Ensure int storage is correct
                $value = (int) $value;
            }
        }

        return $value;
    }

    /**
     * Retrieve all attributes stored into the MongoDB collection for a given product and
     * append their values to the product
     *
     * @param Mage_Catalog_Model_Product $object The product to be loaded
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product Self reference
     */
    protected function _loadModelAttributes($object)
    {
        parent::_loadModelAttributes($object);

        if ($object->getEntityId()) {
            // Retrieve the collection to be updated
            // Provide a raw MongoCollection object pointing
            // to catalog_product_entity collection
            $collection = $this->_getDocumentCollection();

            // We load :
            // - object current store attributes values
            // - default store attributes values
            $storeIds = array('attr_' . $this->getDefaultStoreId());

            if ($object->getStoreId() && $object->getStoreId() != $this->getDefaultStoreId()) {
                $storeIds[] = 'attr_' . $object->getStoreId();
            }


            // Build filter on the product id
            $loadFilter = $this->getIdsFilter($object->getId());

            // Load document from collection
            $loadedData = $collection->findOne($loadFilter, $storeIds);

            // Merge document fields :
            //    if field exists into current store use it
            //    else look for it into the default store
            $attributeData = array();

            if ($loadedData) {
                foreach ($loadedData as $storeId => $attributeValues) {
                    if (in_array($storeId, $storeIds)) {
                        if (!is_array($attributeValues)) {
                            $attributeValues = array($storeId=>$attributeValues);
                        }
                        foreach ($attributeValues as $attributeCode => $value) {
                            $attributeData[$attributeCode] = $value;
                        }
                    }
                }
            }

            // Append loaded attributes to the product
            $object->addData($attributeData);
        }

        return $this;
    }


    /**
     * Delete the document from MongoDB afer the product have been saved
     *
     * @param Mage_Catalog_Model_Product $object The deleted product
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product Self reference
     */
    protected function _afterDelete(Varien_Object $object)
    {

        parent::_afterDelete($object);

        $id = null;

        if (is_numeric($object)) {
            $id = (int)$object;
        } elseif ($object instanceof Varien_Object) {
            $id = (int)$object->getId();
        }

        if (!is_null($id)) {
            $removeFilter = $this->getIdsFilter($id);

            $collection = $this->_getDocumentCollection();
            $collection->remove($removeFilter, array('justOne' => false));
        }

        return $this;
    }


    /**
     * Get the list of images used for attributes small_image, thumbnail and image for a product
     *
     * @param Mage_Catalog_Model_Product $product  The product we want assigned images from
     * @param int|array                  $storeIds The stores that should be filtered
     *                                             (only assignment for these stores will be returned)
     *
     * @return array The list of the images used
     */
    public function getAssignedImages($product, $storeIds)
    {

        // Prepare an empty array for the result
        $result = array();

        if (!is_array($storeIds)) {
            // Store ids filter have to be an array
            // Make an array if it is an integer
            $storeIds = array($storeIds);
        }

        if (Mage::app()->isSingleStoreMode()) {
            // If only one store is present : automatically append default store
            // since everyting sould be filled into this store
            if (Mage::app()->isSingleStoreMode()) {
                $storeIds = array_merge(array(0), $storeIds);
            }
        }

        // Prepare document load filter
        $loadFilter = $this->getIdsFilter($product->getId());

        // Prepare the field select like an array of scoped attributes field name
        $fieldSelect = array();
        $imageAttributes = array('small_image', 'thumbnail', 'image');

        foreach ($storeIds as $storeId) {
            foreach ($imageAttributes as $imageAttribute) {
                $fieldSelect['attr_' . $storeId  . '.' . $imageAttribute] = 1;
            }
        }

        // Load the document from MongoDB
        $it = $this->_getDocumentCollection()
            ->find($loadFilter, $fieldSelect);

        // Iterate through the loaded document to match awaited format
        while ($it->hasNext()) {
            $data = $it->getNext();
            foreach ($data as $key => $value) {
                $storeData = sscanf($key, 'attr_%d');
                if (is_int($storeData[0])) {
                    $storeId = $storeData[0];
                    foreach ($value as $attributeCode => $attributeValue) {
                        $result[] = array(
                            'filepath'       => $attributeValue,
                            'store_id'       => $storeId,
                            'attribute_code' => $attributeCode
                        );
                    }
                }
            }
        }

        return $result;
    }


    /**
     * Load value for a precise product attribute into the given store
     *
     * @param int    $productId     Id of the product we want to load the attribute value
     * @param string $attributeCode Attribute code we want to know the value
     * @param int    $storeId       Store Id we want to know the value. If not given admin store id is used
     *
     * @return mixed The value of the attribute for the product into the current store
     */
    public function getProductAttributeValue($productId, $attributeCode, $storeId = false)
    {
        $collection = $this->_getDocumentCollection();
        $loadFilter = $this->getIdsFilter($productId);
        $defaultStoreId = $this->getDefaultStoreId();

        $attributeIndexes = array('attr_' . $defaultStoreId . '.' . $attributeCode);

        if ($storeId !== false && $storeId != $defaultStoreId) {
            $attributeIndexes[] = 'attr_' .  $storeId . '.' . $attributeCode;
        }

        $data = $collection->findOne($loadFilter, $attributeIndexes);

        $result = null;

        if ($storeId !== false && isset($data['attr_' . $storeId]) && isset($data['attr_' . $storeId][$attributeCode])) {
            $result = $data['attr_' . $storeId][$attributeCode];
        } elseif (isset($data['attr_' . $defaultStoreId]) && isset($data['attr_' . $defaultStoreId][$attributeCode])) {
            $result = $data['attr_' . $defaultStoreId][$attributeCode];
        }

        return $result;

    }

    /**
     * Attributes mass update for a set of product ids.
     * Update data is an array with attribute_code as key and new value of the attribute as value
     * It may contains several attributes to be updated at the same time
     *
     * @param array $entityIds  The list of product ids to be updated
     * @param array $updateData The new values of the updated attribute(s)
     * @param int   $storeId    The scope of the mass update
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product Self reference
     */
    public function massDataUpdate($entityIds, $updateData, $storeId)
    {
        $collection = $this->_getDocumentCollection();
        $updateCond = $this->getIdsFilter($entityIds);

        foreach ($updateData as $attributeCode => $value) {

            $attribute = $this->getAttribute($attributeCode);

            if ($attribute) {
                $value = $this->_prepareValueForDocumentSave($attribute, $value);
            }

            $attrIndex = 'attr_' . $storeId . '.' . $attributeCode;

            $collection->update(
                $updateCond,
                array('$set' => array($attrIndex => $value)),
                array('multiple' => true)
            );
        }

        return $this;
    }

    /**
     * Update a product field on target product ids
     *
     * @param array  $productIds The product ids
     * @param string $fieldName  The field name
     * @param string $fieldValue The new value
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product
     */
    public function updateProductField($productIds, $fieldName, $fieldValue)
    {
        $updateCond = $this->getIdsFilter($productIds);
        return $this->updateProductFieldFromFilter($updateCond, $fieldName, $fieldValue);
    }

    /**
     * Update a document field conditionnally
     *
     * @param string $updateCond The update criteria
     * @param string $fieldName  The field name to update
     * @param string $fieldValue The new value
     *
     * @return Smile_MongoCore_Model_Resource_Override_Catalog_Product
     */
    public function updateProductFieldFromFilter($updateCond, $fieldName, $fieldValue)
    {
        $collection = $this->_getDocumentCollection();
        $updateData = array('$set' => array($fieldName => $fieldValue));
        $collection->update($updateCond, $updateData, array('multiple' => true));
        return $this;
    }

    /**
     * Update product document with raw data
     *
     * @param int    $productId  Id of the product to be updated
     * @param array  $updateData Data to be updated
     * @param string $operator   Mongo Operator to be used ($set by default)
     *
     * @return Smile_MongoCatalog_Model_Resource_Override_Catalog_Product
     */
    public function updateRawDocument($productId, $updateData, $operator = '$set')
    {
        $collection = $this->_getDocumentCollection();
        $updateCond = $this->getIdsFilter($productId);

        if ($operator) {
            $updateData = array($operator => $updateData);
        }

        $collection->update($updateCond, $updateData, array('upsert' => true));

        return $this;
    }

    /**
     * Explicit save of an attribute : we first save the related product in Mongo to ensure the attribute
     * is correctly updated there as well as in MySQL db
     *
     * @param Varien_Object $object        object
     * @param string        $attributeCode attribute code
     *
     * @throws Exception
     * @return $this
     */
    public function saveAttribute(Varien_Object $object, $attributeCode)
    {
        $attribute      = $this->getAttribute($attributeCode);
        $newValue       = $object->getData($attributeCode);

        if ($attribute->isValueEmpty($newValue)) {
            $newValue = null;
        }

        // Retrieve the collection to be updated
        // Provide a raw MongoCollection object pointing
        // to catalog_product_entity collection
        $collection = $this->_getDocumentCollection();

        // We update only the document matching the currently edited product
        $updateCond = $this->getIdsFilter($object->getId());

        // By default => attribute data sould be stored into the product current store scope
        $storeId = $object->getStoreId();
        $updateData = array();

        if (!$attribute->isScopeWebsite() && ($storeId != Mage_Core_Model_App::ADMIN_STORE_ID)) {
            if ($object->isObjectNew() === true || $attribute->isScopeGlobal()) {
                // If product is new we store it into the default store instead of the current one
                $storeId = 'attr_' . $this->getDefaultStoreId();
            } else {
                $storeId = 'attr_' . $storeId;
            }

            if (!is_string($newValue) || $newValue != '') {
                // Push saved values into the saved document
                $fieldName = $storeId . '.' . $attribute->getAttributeCode();
                $updateData[$fieldName] = $this->_prepareValueForDocumentSave($attribute, $newValue);
            }

            $collection->update($updateCond, array('$set' => $updateData), array('upsert' => true));
        } else {
            $store           = Mage::app()->getStore($storeId);
            $websiteStoreIds = $store->getWebsite()->getStoreIds();

            foreach ($websiteStoreIds as $storeId) {
                // Push saved values into the saved document
                $attrIndex = 'attr_' . $storeId . '.' . $attributeCode;

                $collection->update(
                    $updateCond,
                    array('$set' => array($attrIndex => $newValue)),
                    array('multiple' => true)
                );
            }
        }

        $object->isObjectNew(false);

        if (in_array($attributeCode, $this->getSqlAttributesCodes())) {
            return parent::saveAttribute($object, $attributeCode);
        }

        return $this;

    }
}