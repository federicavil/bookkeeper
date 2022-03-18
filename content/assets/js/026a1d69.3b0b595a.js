"use strict";(self.webpackChunksite_3=self.webpackChunksite_3||[]).push([[7886],{3905:function(e,t,n){n.d(t,{Zo:function(){return s},kt:function(){return f}});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},i=Object.keys(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)n=i[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var p=r.createContext({}),c=function(e){var t=r.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},s=function(e){var t=c(e.components);return r.createElement(p.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,i=e.originalType,p=e.parentName,s=l(e,["components","mdxType","originalType","parentName"]),m=c(n),f=a,h=m["".concat(p,".").concat(f)]||m[f]||u[f]||i;return n?r.createElement(h,o(o({ref:t},s),{},{components:n})):r.createElement(h,o({ref:t},s))}));function f(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var i=n.length,o=new Array(i);o[0]=m;var l={};for(var p in t)hasOwnProperty.call(t,p)&&(l[p]=t[p]);l.originalType=e,l.mdxType="string"==typeof e?e:a,o[1]=l;for(var c=2;c<i;c++)o[c]=n[c];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},8906:function(e,t,n){n.r(t),n.d(t,{contentTitle:function(){return p},default:function(){return m},frontMatter:function(){return l},metadata:function(){return c},toc:function(){return s}});var r=n(7462),a=n(3366),i=(n(7294),n(3905)),o=["components"],l={},p="BP-XYZ: caption of bookkeeper proposal",c={type:"mdx",permalink:"/bps/BP-template",source:"@site/src/pages/bps/BP-template.md",title:"BP-XYZ: caption of bookkeeper proposal",description:"Motivation",frontMatter:{}},s=[{value:"Motivation",id:"motivation",level:3},{value:"Public Interfaces",id:"public-interfaces",level:3},{value:"Proposed Changes",id:"proposed-changes",level:3},{value:"Compatibility, Deprecation, and Migration Plan",id:"compatibility-deprecation-and-migration-plan",level:3},{value:"Test Plan",id:"test-plan",level:3},{value:"Rejected Alternatives",id:"rejected-alternatives",level:3}],u={toc:s};function m(e){var t=e.components,n=(0,a.Z)(e,o);return(0,i.kt)("wrapper",(0,r.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h1",{id:"bp-xyz-caption-of-bookkeeper-proposal"},"BP-XYZ: caption of bookkeeper proposal"),(0,i.kt)("h3",{id:"motivation"},"Motivation"),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"Describe the problems you are trying to solve")),(0,i.kt)("h3",{id:"public-interfaces"},"Public Interfaces"),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"Briefly list any new interfaces that will be introduced as part of this proposal or any existing interfaces that will be removed or changed. The purpose of this section is to concisely call out the public contract that will come along with this feature.")),(0,i.kt)("p",null,"A public interface is any change to the following:"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"Data format, Metadata format"),(0,i.kt)("li",{parentName:"ul"},"The wire protocol and api behavior"),(0,i.kt)("li",{parentName:"ul"},"Any class in the public packages"),(0,i.kt)("li",{parentName:"ul"},"Monitoring"),(0,i.kt)("li",{parentName:"ul"},"Command line tools and arguments"),(0,i.kt)("li",{parentName:"ul"},"Anything else that will likely break existing users in some way when they upgrade")),(0,i.kt)("h3",{id:"proposed-changes"},"Proposed Changes"),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"Describe the new thing you want to do in appropriate detail. This may be fairly extensive and have large subsections of its own. Or it may be a few sentences. Use judgement based on the scope of the change.")),(0,i.kt)("h3",{id:"compatibility-deprecation-and-migration-plan"},"Compatibility, Deprecation, and Migration Plan"),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"What impact (if any) will there be on existing users? "),(0,i.kt)("li",{parentName:"ul"},"If we are changing behavior how will we phase out the older behavior? "),(0,i.kt)("li",{parentName:"ul"},"If we need special migration tools, describe them here."),(0,i.kt)("li",{parentName:"ul"},"When will we remove the existing behavior?")),(0,i.kt)("h3",{id:"test-plan"},"Test Plan"),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"Describe in few sentences how the BP will be tested. We are mostly interested in system tests (since unit-tests are specific to implementation details). How will we know that the implementation works as expected? How will we know nothing broke?")),(0,i.kt)("h3",{id:"rejected-alternatives"},"Rejected Alternatives"),(0,i.kt)("p",null,(0,i.kt)("em",{parentName:"p"},"If there are alternative ways of accomplishing the same thing, what were they? The purpose of this section is to motivate why the design is the way it is and not some other way.")))}m.isMDXComponent=!0}}]);