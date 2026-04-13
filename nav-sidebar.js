/* nav-sidebar.js – Keyboard and aria-expanded support for the Services submenu */
(function () {
  'use strict';

  function initNavSidebar() {
    var triggers = document.querySelectorAll('.nav-sidebar__item--has-sub .nav-sidebar__link[aria-haspopup]');
    triggers.forEach(function (trigger) {
      var item = trigger.closest('.nav-sidebar__item--has-sub');
      if (!item) return;

      /* Toggle expanded state on Enter / Space key press */
      trigger.addEventListener('keydown', function (e) {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          var isExpanded = trigger.getAttribute('aria-expanded') === 'true';
          trigger.setAttribute('aria-expanded', String(!isExpanded));
          var submenu = item.querySelector('.nav-sidebar__submenu');
          if (submenu) {
            submenu.style.maxHeight = isExpanded ? '0' : '300px';
          }
        }
      });

      /* Sync aria-expanded with CSS :hover/:focus-within via pointer events */
      item.addEventListener('mouseenter', function () {
        trigger.setAttribute('aria-expanded', 'true');
      });
      item.addEventListener('mouseleave', function () {
        /* Only collapse if focus is not inside */
        if (!item.contains(document.activeElement)) {
          trigger.setAttribute('aria-expanded', 'false');
        }
      });
      item.addEventListener('focusin', function () {
        trigger.setAttribute('aria-expanded', 'true');
      });
      item.addEventListener('focusout', function (e) {
        /* Collapse only when focus leaves the item entirely */
        if (!item.contains(e.relatedTarget)) {
          trigger.setAttribute('aria-expanded', 'false');
        }
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initNavSidebar);
  } else {
    initNavSidebar();
  }
})();