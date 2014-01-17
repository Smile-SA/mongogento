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
 * Util class allowing to index product attributes from MongoDB
 *
 * @category  Smile
 * @package   Smile_MongoSearch
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoSearch_Model_Resource_Override_EnterpriseCatalogSearch_Index_Action_Fulltext_Refresh
    extends Enterprise_CatalogSearch_Model_Index_Action_Fulltext_Refresh
{


    /**
     * Load product(s) attributes
     *
     * @param int   $storeId        Store id we want the attribute for
     * @param array $productIds     Product ids we want the attribute for
     * @param array $attributeTypes Attribute ids to get the value for by table
     *
     * @return array
     */
    protected function _getProductAttributes($storeId, array $productIds, array $attributeTypes)
    {
        return Mage::getResourceSingleton('mongosearch/product')->getProductAttributes($storeId, $productIds, $attributeTypes);
    }


}