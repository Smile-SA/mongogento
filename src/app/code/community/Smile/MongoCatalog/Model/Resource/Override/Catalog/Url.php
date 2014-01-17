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
 * Product URLs rewrite MongoDB Compatibility
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCatalog_Model_Resource_Override_Catalog_Url extends Mage_Catalog_Model_Resource_Url
{

    /**
     * Save product attribute
     *
     * @param Varien_Object $product       Product we want to save the attribute for
     * @param string        $attributeCode Attribute code to be saved
     *
     * @return Mage_Catalog_Model_Resource_Url
     */
    public function saveProductAttribute(Varien_Object $product, $attributeCode)
    {
        parent::saveProductAttribute($product, $attributeCode);

        $adapter = Mage::getSingleton('mongocore/resource_connection_adapter');
        $updateCond = $adapter->getQueryBuilder()->getIdsFilter($product->getId());
        $updateField = sprintf('attr_%d_%s', $product->getStoreId(), $attributeCode);
        $attributeValue = $product->getData($attributeCode);

        Mage::getResourceModel('catalog/product')->updateProductFieldFromFilter($updateCond, $updateField, $attributeValue);

        return $this;
    }


}