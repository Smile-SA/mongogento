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
 * Catalog Product Import using MongoDB
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Aurelien FOUCRET <aufou@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCatalog_Model_Import_Entity_Product extends Mage_ImportExport_Model_Import_Entity_Product
{
    /**
     * Save product attributes.
     *
     * @param array $attributesData Attribute to be saved into products
     *
     * @return Mage_ImportExport_Model_Import_Entity_Product
     */
    protected function _saveProductAttributes(array $attributesData)
    {
        $sqlAttributeCodes = Mage::getResourceModel('catalog/product')->getSqlAttributesCodes();

        $sqlAttributeIdByCode = array();
        $docData   = array();

        foreach ($attributesData as $tableName => $skuData) {
            $tableData = array();

            foreach ($skuData as $sku => $attributes) {
                $productId = $this->_newSku[$sku]['entity_id'];

                foreach ($attributes as $attributeId => $storeValues) {

                    if (!isset($sqlAttributeIdByCode[$attributeId])) {
                        $sqlAttributeIdByCode[$attributeId] = Mage::getModel('eav/entity_attribute')->load($attributeId);
                    }

                    $attributeCode = $sqlAttributeIdByCode[$attributeId]->getAttributeCode();

                    $sqlAttribute = false;

                    if (in_array($attributeCode, $sqlAttributeCodes)) {
                        $sqlAttribute = true;
                    }

                    foreach ($storeValues as $storeId => $storeValue) {

                        if ($sqlAttribute) {
                            $tableData[] = array(
                                'entity_id'      => $productId,
                                'entity_type_id' => $this->_entityTypeId,
                                'attribute_id'   => $attributeId,
                                'store_id'       => $storeId,
                                'value'          => $storeValue
                            );
                        }

                        $docData[$productId]['attr_' . $storeId . '.' . $attributeCode] = $storeValue;
                    }

                    if (!isset($docData[$productId])) {
                        $docData[$productId] = array();
                    }
                }

            }

            if (!empty($tableData)) {
                $this->_connection->insertOnDuplicate($tableName, $tableData, array('value'));
            }
        }

        Mage::log($docData);

        foreach ($docData as $productId => $currentDocData) {
            Mage::getResourceModel('catalog/product')->updateRawDocument($productId, $currentDocData, '$set');
        }

        return $this;
    }

}