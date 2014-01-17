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
 * This class allow developers to build queries for MongoDB
 *
 * @category  Smile
 * @package   Smile_MongoCore
 * @author    Aurelien FOUCRET <aurelien.foucret@smile.fr>
 * @copyright 2013 Smile (http://www.smile-oss.com/)
 * @license   http://opensource.org/licenses/afl-3.0.php  Academic Free License (AFL 3.0)
 */
class Smile_MongoCore_Model_Resource_Connection_Query_Builder
{
    /**
     * Build a filter on an array of integer ids or on a single id
     *
     * @param int|array $ids Id the filter must target
     *
     * @return array
     */
    public function getIdsFilter($ids)
    {
        $result = array();

        if (is_array($ids)) {

            foreach ($ids as $position => $entityId) {
                $ids[$position] = new MongoInt32($entityId);
            }

            $result = array('_id' => array('$in' => $ids));

        } else {
            $result = array('_id' => new MongoInt32($ids));
        }

        return $result;
    }
}
