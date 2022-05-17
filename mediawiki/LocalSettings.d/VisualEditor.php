<?php

// https://www.mediawiki.org/wiki/Extension:VisualEditor
## VisualEditor Extension
wfLoadExtension( 'VisualEditor' );
wfLoadExtension( 'Parsoid', "{$IP}/vendor/wikimedia/parsoid/extension.json" );

$wgVirtualRestConfig['modules']['parsoid'] = array(
    'url' => 'http://localhost' . $wgScriptPath . '/rest.php',
);