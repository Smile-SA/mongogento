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
 * Overriden Configurable product collection, because native one explicitely inherits SQL based collection
 * We must ensure this class inherits the mongoDB based collection to process queries correctly
 *
 * @category  Snowleader
 * @package   Snowleader_MongoCatalog
 * @author    Romain Ruaud <romain.ruaud@smile.fr>
 * @copyright 2015 Smile
 * @license   http://opensource.org/licenses/osl-3.0.php  Open Software License (OSL 3.0)
 */
class Smile_MongoCatalog_Model_Resource_Override_Catalog_Product_Type_Configurable_Product_Collection
    extends Smile_MongoCatalog_Model_Resource_Override_Catalog_Product_Collection
{
    /**
     * Link table name
     *
     * @var string
     */
    protected $_linkTable;

    /**
     * Assign link table name
     *
     * @return void Nothing
     */
    protected function _construct()
    {
        parent::_construct();
        $this->_linkTable = $this->getTable('catalog/product_super_link');
    }

    /**
     * Init select
     *
     * @return Mage_Catalog_Model_Resource_Product_Type_Configurable_Product_Collection
     */
    protected function _initSelect()
    {
        parent::_initSelect();
        $this->getSelect()->join(
            array('link_table' => $this->_linkTable),
            'link_table.product_id = e.entity_id',
            array('parent_id')
        );

        return $this;
    }

    /**
     * Set Product filter to result
     *
     * @param Mage_Catalog_Model_Product $product The product
     *
     * @return Mage_Catalog_Model_Resource_Product_Type_Configurable_Product_Collection
     */
    public function setProductFilter($product)
    {
        $this->getSelect()->where('link_table.parent_id = ?', (int) $product->getId());
        return $this;
    }

    /**
     * Retrieve is flat enabled flag
     * Return alvays false if magento run admin
     *
     * @return bool
     */
    public function isEnabledFlat()
    {
        return false;
    }
}