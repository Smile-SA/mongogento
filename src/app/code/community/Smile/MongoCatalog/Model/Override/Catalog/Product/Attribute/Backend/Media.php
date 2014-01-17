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
 * Media gallery backend model modified to handle allery from MongoDB
 *
 * @category  Smile
 * @package   Smile_MongoCatalog
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCatalog_Model_Override_Catalog_Product_Attribute_Backend_Media
    extends Mage_Catalog_Model_Product_Attribute_Backend_Media
{

    /**
     * Load gallery for a given product
     *
     * @param Mage_Catalog_Model_Product $object The product we should load the gallery for
     *
     * @return void othing is returned by this method
     */
    public function afterLoad($object)
    {
        // Get the attribute code for the gallery
        $attrCode = $this->getAttribute()->getAttributeCode();

        // Prepare result formatted as awaited by others function
        $value = array();
        $value['images'] = array();
        $value['values'] = array();
        $localAttributes = array('label', 'position', 'disabled');

        foreach ($this->_getResource()->loadData($object, $attrCode) as $image) {

            // Loop over the gallery and append the image to the gallery
            if (isset($image['attr_' . $object->getStoreId()])) {
                $image = array_merge($image, $image['attr_' . $object->getStoreId()]);
            } else if (isset($image['attr_' . Mage_Core_Model_App::ADMIN_STORE_ID])) {
                 $image = array_merge($image, $image['attr_' . Mage_Core_Model_App::ADMIN_STORE_ID]);
            }

            foreach ($localAttributes as $localAttribute) {
                if (!isset($image[$localAttribute]) || is_null($image[$localAttribute])) {
                    $image[$localAttribute] = $this->_getDefaultValue($localAttribute, $image);
                }
            }

            if (isset($image['attr_' . Mage_Core_Model_App::ADMIN_STORE_ID])) {
                $position = $image['attr_' . Mage_Core_Model_App::ADMIN_STORE_ID]["position"];
                $value['images'][$position] = $image;
            } else {
                $value['images'][] = $image;
            }
        }

        // We sort images here, because they can have been imported with bad order
        $imagesToSort = $value['images'];
        $sortedImages = array();
        ksort($imagesToSort);
        foreach ($imagesToSort as $sortedImage) {
            $sortedImages[] = $sortedImage;
        }
        $value['images'] = $sortedImages;
        // Set the gallery into the product
        $object->setData($attrCode, $value);
    }


    /**
     * Before saving the object we store the gallery data into a temporary field to avoid
     * the gallery is saved as a standard attribute
     *
     * @param Mage_Catalog_Model_Product $object The saved product
     *
     * @return void othing is returned by this method
     */
    public function beforeSave($object)
    {
        parent::beforeSave($object);

        $attrCode = $this->getAttribute()->getAttributeCode();
        $object->setData($attrCode.'_tmp', $object->getData($attrCode));
        $object->unsetData($attrCode);
    }

    /**
     * Since using MongoDB, product gallery should not be saved in the same way
     * we do with MySQL. We can use a more simple way to format the gallery.
     *
     * @param Mage_Catalog_Model_Product $object The saved product
     *
     * @return void othing is returned by this method
     */
    public function afterSave($object)
    {
        // Get the attribute code for the gallery
        $attrCode = $this->getAttribute()->getAttributeCode();

        // Retrieve gallery data saved into temporary field into self::beforeSave
        $data = $object->getData($attrCode . '_tmp');

        // Load gallery old data
        // It is required since we don't want to erase other websites data (label, position, exclude)
        $galleryData = $this->_getResource()->loadData($object, $attrCode);
        $sortedImages = array();

        if (isset($data['images'])) {
            // Loop over the new image galery data to build the new gallery
            foreach ($data['images'] as $imageData) {
                // A md5 hash is used as unique id since we can not have a incremet value_id
                $imageHash = md5($imageData['file']);
                $position = (int) $imageData['position'];

                if (!empty($imageData['removed'])) {
                    // Image have been removed
                    // Delete from old gallery
                    // TODO : remove physical files ?
                    unset($galleryData[$imageHash]);

                } else {
                    // Insert or update the image and it's new value (label, position, exclude)
                    $storeData = array(
                        'label'    => $imageData['label'],
                        'position' => $position,
                        'disabled' => $imageData['disabled']
                    );
                    $storeDataKey = 'attr_' . $object->getStoreId();

                    $galleryData[$imageHash] = array(
                        'file'          => $imageData['file'],
                        'value_id'      => $imageHash,
                         $storeDataKey  => $storeData
                    );

                    if (!isset($galleryData[$imageHash]['attr_' . Mage_Core_Model_App::ADMIN_STORE_ID])) {
                        // If no default values are set => do it
                        $galleryData[$imageHash]['attr_' . Mage_Core_Model_App::ADMIN_STORE_ID] = $storeData;
                    }

                    $sortedImages[$imageHash] = $position;
                }
            }
        }
        // Before saving the gallery we sort the images contained into it
        $savedGallery = array();
        asort($sortedImages);

        foreach ($sortedImages as $imageHash => $position) {
            $savedGallery[$imageHash] = $galleryData[$imageHash];
        }

        // Save the new gallery field (warning : discards the hash key)
        $this->_getResource()->saveGallery($object, $attrCode, $savedGallery);
    }


    /**
     * Retrieve resource model
     *
     * @return Mage_Catalog_Model_Resource_Eav_Mysql4_Product_Attribute_Backend_Media
     */
    protected function _getResource()
    {
        return Mage::getResourceSingleton('mongocatalog/override_catalog_product_attribute_backend_media');
    }
}