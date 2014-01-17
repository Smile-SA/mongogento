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
 * Events reletated code for MongoCatalog module
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Aurelien FOUCRET <aufou@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCatalog_Model_Observer extends Mage_Catalog_Model_Resource_Product
{
    /**
     * Retrieve product ids saved from the category products back office panel and update their category
     *
     * @param Varien_Event_Observer $observer Observer
     *
     * @return Smile_MongoCatalog_Model_Observer
     */
    public function saveUpdatedCategories(Varien_Event_Observer $observer)
    {
        $updatedProductIds = $observer->getEvent()->getData('product_ids');
        $categoryId = $observer->getEvent()->getData('category')->getId();
        Mage::getResourceModel('catalog/product')->saveChangedCategory($categoryId, $updatedProductIds);

        return $this;
    }
}